package riscvstorage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.log4j.Logger;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsReportPartitioning;
import org.apache.spark.sql.connector.read.partitioning.Distribution;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.json4s.jackson.JsonMethods$;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

import tpch.TpchSchemaProvider$;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;


public class DefaultSource implements TableProvider {
    public static String getTableName(String path) {
        String[] names = path.split("/");
        return names[names.length - 1];
    }

    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return TpchSchemaProvider$.MODULE$.getSchema(getTableName(options.get("path")));
    }

    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new RISCVBatchTable(schema, properties.get("path"));
    }
}

class RISCVBatchTable implements Table, SupportsRead {
    private final StructType schema_;
    private final String path_;

    public RISCVBatchTable(StructType schema, String path) {
        schema_ = schema;
        path_ = path;
    }

    public String name() {
        return DefaultSource.getTableName(path_) + "@" + path_;
    }

    public StructType schema() {
        return schema_;
    }

    public Set<TableCapability> capabilities() {
        return new HashSet<>(Collections.singletonList(TableCapability.BATCH_READ));
    }

    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new RISCVScanBuilder(schema_, path_);
    }
}

class RISCVScanBuilder implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns {
    public static final List<Class<? extends Filter>> unsupportedFilters = Collections.unmodifiableList(Arrays.asList(IsNull.class, IsNotNull.class, EqualNullSafe.class, And.class, In.class, AlwaysTrue.class, AlwaysFalse.class));
    private final StructType schema_;
    private StructType requiredSchema_;
    private final String path_;

    private Filter[] pushedFilters_;

    public RISCVScanBuilder(StructType schema, String path) {
        schema_ = schema;
        path_ = path;
        pushedFilters_ = new Filter[0];
        RISCVLib lib = RISCVLib.get();
        assert (lib != null);
    }

    boolean testFilter(Filter filter) {
        for (Class<? extends Filter> f : unsupportedFilters) {
            if (f.isInstance(filter)) return false;
        }
        Set<String> refs = new HashSet<>(Arrays.asList(filter.references()));
        if (refs.size() > 1) return false;
        if (filter instanceof Or) {
            Or orFilter = (Or) filter;
            return testFilter(orFilter.left()) && testFilter(orFilter.right());
        } else if (filter instanceof Not) {
            Not notFilter = (Not) filter;
            return (notFilter.child() instanceof EqualTo ||
                    notFilter.child() instanceof StringStartsWith ||
                    notFilter.child() instanceof StringEndsWith ||
                    notFilter.child() instanceof StringContains);
        } else {
            return !filter.containsNestedColumn();
        }
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        ArrayList<Filter> unsupported = new ArrayList<>();
        ArrayList<Filter> supported = new ArrayList<>();
        for (Filter filter : filters) {
            if (testFilter(filter)) {
                supported.add(filter);
            } else {
                unsupported.add(filter);
                Logger.getLogger("riscvstorage.DefaultSource").info("Not supported: " + JsonMethods$.MODULE$.asJsonNode(FiltersJson$.MODULE$.jsonValue(filter)).toString());
            }
        }

        pushedFilters_ = supported.toArray(pushedFilters_);
        return unsupported.toArray(new Filter[0]);
    }

    @Override
    public Filter[] pushedFilters() {
        return pushedFilters_;
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        requiredSchema_ = requiredSchema;
    }

    @Override
    public Scan build() {
        return new RISCVBatchScan(path_, schema_, requiredSchema_, pushedFilters_);
    }
}

class RISCVPartitioning implements Partitioning {
    private final int num_;

    public RISCVPartitioning(int num) {
        num_ = num;
    }

    @Override
    public int numPartitions() {
        return num_;
    }

    @Override
    public boolean satisfy(Distribution distribution) {
        return false;
    }
}

class RISCVBatchScan implements Scan, Batch, SupportsReportPartitioning {
    private static final String[] filterStrs_ = {"Or", "EqualTo", "NotEqualTo", "GreaterThan", "GreaterThanOrEqual", "LessThan", "LessThanOrEqual", "StringStartsWith", "NotStringStartsWith", "StringEndsWith", "NotStringEndsWith", "StringContains", "NotStringContains"};
    private static final int strFilterIdOffset_ = 6;
    private static final String[] typeStrs_ = {"integer", "date", "decimal", "string"};

    private final String path_;
    private final ArrayNode fieldsNode_;
    private final StructType readSchema_;
    private final int numPartitions_;

    public RISCVBatchScan(String path, StructType schema, StructType requiredSchema, Filter[] filters) {
        // We should build RISCV program here, as the schema and filter are clear
        path_ = path;
        fieldsNode_ = fieldsNode(schema, requiredSchema, filters);
        readSchema_ = requiredSchema;

        assert (path.startsWith("file:///")); // Only support local RISCV Table
        String realpath = path.substring(7);
        try {
            Scanner in = new Scanner(new File(realpath + "/.meta"));
            numPartitions_ = in.nextInt();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new IllegalArgumentException(realpath + "/.meta does not exist!");
        }
    }

    private static ArrayList<Filter> flatternOr(Filter filter) {
        ArrayList<Filter> ans = new ArrayList<>();
        if (filter instanceof Or) {
            Or or = (Or) filter;
            ans.addAll(flatternOr(or.left()));
            ans.addAll(flatternOr(or.right()));
        } else {
            ans.add(filter);
        }
        return ans;
    }

    private static ObjectNode filterNode(Filter filter, ObjectMapper mapper, int typeId) {
        HashMap<String, Integer> filterIds = new HashMap<String, Integer>();
        int id = 0;
        for (String s : filterStrs_) {
            id += 1;
            filterIds.put(s, id);
        }

        boolean isdate = typeStrs_[typeId].equals("date");
        boolean isdecimal = typeStrs_[typeId].equals("decimal");
        boolean isstring = typeStrs_[typeId].equals("string");
        ObjectNode fo = mapper.createObjectNode();
        if (filter instanceof Or) {
            ArrayNode fa = mapper.createArrayNode();
            ArrayList<Filter> filters = flatternOr(filter);
            for (Filter child : filters) {
                fa.add(filterNode(child, mapper, typeId));
            }
            fo.set(String.valueOf(filterIds.get("Or")), fa);
        } else {
            ObjectNode fnode = (ObjectNode) JsonMethods$.MODULE$.asJsonNode(FiltersJson$.MODULE$.jsonValue(filter instanceof Not ? ((Not) filter).child() : filter));
            if (isdate) {
                String[] datestr = fnode.get("value").textValue().split("-");
                fnode.remove("value");
                fnode.put("value", LocalDate.of(Integer.valueOf(datestr[0]), Integer.valueOf(datestr[1]), Integer.valueOf(datestr[2])).toEpochDay());
            } else if (isdecimal) {
                float f = Float.valueOf(fnode.get("value").textValue());
                fnode.remove("value");
                fnode.put("value", Math.round(100 * f));
            }
            String type = filter instanceof Not ? "Not" + fnode.get("type").textValue() : fnode.get("type").textValue();
            fo.set(String.valueOf(filterIds.get(type) + (isstring ? strFilterIdOffset_ : 0)), fnode.get("value"));
        }
        return fo;
    }

    private static ArrayNode fieldsNode(StructType schema, StructType requiredSchema, Filter[] filters) {
        ArrayNode fields = (ArrayNode) JsonMethods$.MODULE$.asJsonNode(schema.jsonValue()).get("fields");
        ObjectMapper mapper = new ObjectMapper();

        List<String> required_names = Arrays.asList(requiredSchema.fieldNames());
        for (JsonNode field : fields) {
            ObjectNode f = (ObjectNode) field;
            if (f.has("nullable")) {
                f.remove("nullable");
            }
            if (f.has("metadata")) {
                f.remove("metadata");
            }
            f.put("required", required_names.contains(f.get("name").textValue()));
            String ftype = f.get("type").textValue();
            f.remove("type");
            int id = typeStrs_.length;
            for (int i = 0; i < typeStrs_.length; i++) {
                if (ftype.startsWith(typeStrs_[i])) {
                    id = i;
                }
            }
            assert (id != typeStrs_.length);
            f.put("type", id);

            ArrayNode fs = mapper.createArrayNode();
            for (Filter filter : filters) {
                Set<String> refs = new HashSet<>(Arrays.asList(filter.references()));
                String column = refs.stream().findFirst().get();
                if (column.equals(f.get("name").textValue())) {
                    fs.add(filterNode(filter, mapper, f.get("type").asInt()));
                }
            }
            f.set("filters", fs);
            if (fs.size() == 0 && !f.get("required").asBoolean()) {
                f.remove("type");
                f.put("type", typeStrs_.length);
            }
        }
        return fields;
    }

    @Override
    public StructType readSchema() {
        return readSchema_;
    }

    @Override
    public Batch toBatch() {
        return this;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        InputPartition[] partitions = new RISCVInputPartition[numPartitions_];
        for (int i = 0; i < numPartitions_; i++) {
            partitions[i] = new RISCVInputPartition(path_ + String.format("/%05d.tbl", i));
        }
        return partitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new RISCVPartitionReaderFactory(fieldsNode_);
    }

    @Override
    public Partitioning outputPartitioning() {
        return new RISCVPartitioning(numPartitions_);
    }
}

class RISCVInputPartition implements InputPartition {
    static final long serialVersionUID = 42424242L;

    public final String path_;

    public RISCVInputPartition(String path) {
        path_ = path;
    }
}

class RISCVPartitionReaderFactory implements PartitionReaderFactory {
    static final long serialVersionUID = 4242L;
    ArrayNode fieldsNode_;

    public RISCVPartitionReaderFactory(ArrayNode fieldsNode) {
        fieldsNode_ = fieldsNode;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        return new RISCVPartitionReader(((RISCVInputPartition) partition).path_, fieldsNode_);
    }
}

class RISCVPartitionReader implements PartitionReader<InternalRow> {
    private static final int nullBytes_ = 8;
    private static final int bufferBytes_ = 196 * 1024 * 1024;
    private static final String filePrefix = "file://";
    private final int numOutputFields_;
    private final long riscvReaderPtr_;
    private boolean riscvFinished_;
    private LinkedList<UnsafeRow> rows_;
    private long reader_nanos_;
    private long batch_nanos_;
    private long total_nanos_;


    public RISCVPartitionReader(String path, ArrayNode fieldsNode) {
        int numOutputFields = 0;
        for (JsonNode fnode : fieldsNode) {
            if (fnode.get("required").asBoolean()) {
                numOutputFields += 1;
            }
        }
        numOutputFields_ = numOutputFields;
        String fieldsStr = fieldsNode.toPrettyString();
        assert (path.startsWith(filePrefix));
        String jsonPath = System.getProperty("java.io.tmpdir") + "/" + Integer.toUnsignedString(fieldsStr.hashCode()) + ".json";
        File f = new File(jsonPath);
        try {
            FileWriter fw = new FileWriter(f);
            fw.write(fieldsStr);
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Instant reader_start = Instant.now();
        Logger.getLogger("riscvstorage.DefaultSource").debug("Creating a reader @" + path);
        riscvReaderPtr_ = riscv_create_reader(path.substring(filePrefix.length()), fieldsStr);
        Logger.getLogger("riscvstorage.DefaultSource").debug("Created a reader with ptr=0x" + Long.toHexString(riscvReaderPtr_) + " @" + path + "\n" + fieldsStr + "\n");
        Logger.getLogger("riscvstorage.DefaultSource").info("Created a reader with ptr=0x" + Long.toHexString(riscvReaderPtr_) + " @" + path);
        riscvFinished_ = false;
        rows_ = new LinkedList<UnsafeRow>();

        reader_nanos_ = Duration.between(reader_start, Instant.now()).toNanos();
        batch_nanos_ = 0;
        total_nanos_ = 0;
    }

    private long riscv_create_reader(String path, String fieldStr) {
        return Native.riscv_create_reader(path, fieldStr);
    }

    private int riscv_next_batch(long riscvReaderPtr, ByteBuffer buffer) {
        return Native.riscv_next_batch(riscvReaderPtr, buffer);
    }


    @Override
    public boolean next() throws IOException {
        if (!rows_.isEmpty()) {
            rows_.pop();
            if (!rows_.isEmpty()) return true;
        }
        if (riscvFinished_) return false;

        Instant next_start = Instant.now();
        int num_rows = 0;
        ByteBuffer buffer = null;

        ByteBuffer output_buffer = ByteBuffer.allocateDirect(bufferBytes_).order(ByteOrder.LITTLE_ENDIAN);

        Instant batch_start = Instant.now();
        Logger.getLogger("riscvstorage.DefaultSource").debug("Calling riscv_next_batch for reader 0x" + Long.toHexString(riscvReaderPtr_));
        num_rows = riscv_next_batch(riscvReaderPtr_, output_buffer);
        Logger.getLogger("riscvstorage.DefaultSource").info("Called riscv_next_batch for reader 0x" + Long.toHexString(riscvReaderPtr_) + ", got " + num_rows + " rows");
        batch_nanos_ += Duration.between(batch_start, Instant.now()).toNanos();

        riscvFinished_ = true;
        for (int i = 0; i < num_rows; i++) {
            output_buffer.getInt();
            int rowBytes = output_buffer.getInt();
            if (rowBytes < (numOutputFields_ << 3)) {
                Logger.getLogger("riscvstorage.DefaultSource").error("rowBytes=" + rowBytes + " < " + (numOutputFields_ << 3) + "=(numFields << 3) riscvReaderPtr_=0x" + Long.toHexString(riscvReaderPtr_) + " num_rows=" + num_rows);
            }
            byte[] arr = new byte[nullBytes_ + rowBytes];
            for (int j = 0; j < nullBytes_; j++) {
                arr[j] = 0;
            }
            try {
                output_buffer.get(arr, nullBytes_, rowBytes);
            } catch (BufferUnderflowException e) {
                throw new IOException("Buffer depleted before finish filling all the rows");
            }

            UnsafeRow row = new UnsafeRow(numOutputFields_);
            row.pointTo(arr, nullBytes_ + rowBytes);
            rows_.add(row);
        }
        total_nanos_ += Duration.between(next_start, Instant.now()).toNanos();
        return (num_rows != 0);
    }

    @Override
    public InternalRow get() {
        return rows_.getFirst();
    }

    @Override
    public void close() throws IOException {
        Logger.getLogger("riscvstorage.DefaultSource").info("Reader 0x" + Long.toHexString(riscvReaderPtr_) + " finished:" + " reader_nano " + reader_nanos_ + " batch_nano " + batch_nanos_ + " buffer_nano " + (total_nanos_ - batch_nanos_));
    }
}