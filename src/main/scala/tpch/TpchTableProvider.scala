package tpch
import org.apache.spark.sql.SparkSession

class TpchTableProvider(sparkSession: SparkSession, inputDir:String, useParquetTable:Boolean, useRISCVTable:Boolean) {
    val lineitem = if (useParquetTable) sparkSession.read.parquet(inputDir + "/lineitem")
    else if (useRISCVTable) sparkSession.read.format("riscvstorage").load(inputDir + "/lineitem")
      else sparkSession.read.option("sep", "|").option("header", "false").schema(TpchSchemaProvider.lineitem).csv(inputDir + "/lineitem.tbl")
    val nation = if (useParquetTable) sparkSession.read.parquet(inputDir + "/nation")
    else if (useRISCVTable) sparkSession.read.format("riscvstorage").load(inputDir + "/nation")
      else sparkSession.read.option("sep", "|").option("header", "false").schema(TpchSchemaProvider.nation).csv(inputDir + "/nation.tbl")
    val order = if (useParquetTable) sparkSession.read.parquet(inputDir + "/orders")
    else if (useRISCVTable) sparkSession.read.format("riscvstorage").load(inputDir + "/orders")
      else sparkSession.read.option("sep", "|").option("header", "false").schema(TpchSchemaProvider.order).csv(inputDir + "/orders.tbl")
    val region = if (useParquetTable) sparkSession.read.parquet(inputDir + "/region")
    else if (useRISCVTable) sparkSession.read.format("riscvstorage").load(inputDir + "/region")
      else sparkSession.read.option("sep", "|").option("header", "false").schema(TpchSchemaProvider.region).csv(inputDir + "/region.tbl")
    val part = if (useParquetTable) sparkSession.read.parquet(inputDir + "/part")
    else if (useRISCVTable) sparkSession.read.format("riscvstorage").load(inputDir + "/part")
      else sparkSession.read.option("sep", "|").option("header", "false").schema(TpchSchemaProvider.part).csv(inputDir + "/part.tbl")
    val customer = if (useParquetTable) sparkSession.read.parquet(inputDir + "/customer")
    else if (useRISCVTable) sparkSession.read.format("riscvstorage").load(inputDir + "/customer")
      else sparkSession.read.option("sep", "|").option("header", "false").schema(TpchSchemaProvider.customer).csv(inputDir + "/customer.tbl")
    val supplier = if (useParquetTable) sparkSession.read.parquet(inputDir + "/supplier")
    else if (useRISCVTable) sparkSession.read.format("riscvstorage").load(inputDir + "/supplier")
      else sparkSession.read.option("sep", "|").option("header", "false").schema(TpchSchemaProvider.supplier).csv(inputDir + "/supplier.tbl")
    val partsupp = if (useParquetTable) sparkSession.read.parquet(inputDir + "/partsupp")
    else if (useRISCVTable) sparkSession.read.format("riscvstorage").load(inputDir + "/partsupp")
      else sparkSession.read.option("sep", "|").option("header", "false").schema(TpchSchemaProvider.partsupp).csv(inputDir + "/partsupp.tbl")
}

// customer  739     12G      124
// lineitem  24252   223G     2695
// nation    2       24K
// orders    5393    64G      675
// part      737     5.9G     74
// partsupp  3687    41G      461
// region    2       24K
// supplier  43      764M     43
