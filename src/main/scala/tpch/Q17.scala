package tpch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 17
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q17 extends TpchQuery {

  override def execute(ss: SparkSession, tpchTableProvider: TpchTableProvider, iDirPrefix: String): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    import ss.implicits._
    import tpchTableProvider._

    val flineitem = lineitem.select($"l_partkey", $"l_quantity", $"l_extendedprice")

    if (iDirPrefix != "") {
      val slineitem0 = lineitem.select($"l_partkey", $"l_quantity")
      val slineitem1 = lineitem.select($"l_partkey", $"l_quantity", $"l_extendedprice")
      slineitem0.write.mode("overwrite").parquet(iDirPrefix + "_slineitem0")
      slineitem1.write.mode("overwrite").parquet(iDirPrefix + "_slineitem1")
      return ss.emptyDataFrame
    }

    val fpart = part.filter($"p_brand" === "Brand#23" && $"p_container" === "MED BOX")
      .select($"p_partkey")
      .join(lineitem, $"p_partkey" === lineitem("l_partkey"), "left_outer")

    fpart.groupBy("p_partkey")
      .agg((avg($"l_quantity") * 0.2).as("avg_quantity"))
      .select($"p_partkey".as("key"), $"avg_quantity")
      .join(fpart, $"key" === fpart("p_partkey"))
      .filter($"l_quantity" < $"avg_quantity")
      .agg(sum($"l_extendedprice") / 7.0)
  }

}

//== Physical Plan ==
//*(13) HashAggregate(keys=[], functions=[sum(l_extendedprice#5)], output=[(sum(l_extendedprice) / 7.0)#280])
//+- Exchange SinglePartition
//   +- *(12) HashAggregate(keys=[], functions=[partial_sum(l_extendedprice#5)], output=[sum#284])
//      +- *(12) Project [l_extendedprice#5]
//         +- *(12) SortMergeJoin [key#199L], [p_partkey#64L], Inner, (l_quantity#4 < avg_quantity#196)
//            :- *(6) Sort [key#199L ASC NULLS FIRST], false, 0
//            :  +- Exchange hashpartitioning(key#199L, 200)
//            :     +- *(5) Filter isnotnull(avg_quantity#196)
//            :        +- *(5) HashAggregate(keys=[p_partkey#64L], functions=[avg(l_quantity#4)], output=[key#199L, avg_quantity#196])
//            :           +- *(5) HashAggregate(keys=[p_partkey#64L], functions=[partial_avg(l_quantity#4)], output=[p_partkey#64L, sum#287, count#288L])
//            :              +- *(5) Project [p_partkey#64L, l_quantity#4]
//            :                 +- SortMergeJoin [p_partkey#64L], [l_partkey#1L], LeftOuter
//            :                    :- *(2) Sort [p_partkey#64L ASC NULLS FIRST], false, 0
//            :                    :  +- Exchange hashpartitioning(p_partkey#64L, 200)
//            :                    :     +- *(1) Project [p_partkey#64L]
//            :                    :        +- *(1) Filter ((((isnotnull(p_brand#67) && isnotnull(p_container#70)) && (p_brand#67 = Brand#23)) && (p_container#70 = MED BOX)) && isnotnull(p_partkey#64L))
//            :                    :           +- *(1) FileScan parquet [p_partkey#64L,p_brand#67,p_container#70] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/part], PartitionFilters: [], PushedFilters: [IsNotNull(p_brand), IsNotNull(p_container), EqualTo(p_brand,Brand#23), EqualTo(p_container,MED BOX), IsNotNull(p_partkey)], ReadSchema: struct<p_partkey:bigint,p_brand:string,p_container:string>
//            :                    +- *(4) Sort [l_partkey#1L ASC NULLS FIRST], false, 0
//            :                       +- Exchange hashpartitioning(l_partkey#1L, 200)
//            :                          +- *(3) FileScan parquet [l_partkey#1L,l_quantity#4] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/lineitem], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<l_partkey:bigint,l_quantity:double>
//            +- *(11) Project [p_partkey#64L, l_quantity#4, l_extendedprice#5]
//               +- *(11) SortMergeJoin [p_partkey#64L], [l_partkey#1L], Inner
//                  :- *(8) Sort [p_partkey#64L ASC NULLS FIRST], false, 0
//                  :  +- ReusedExchange [p_partkey#64L], Exchange hashpartitioning(p_partkey#64L, 200)
//                  +- *(10) Sort [l_partkey#1L ASC NULLS FIRST], false, 0
//                     +- Exchange hashpartitioning(l_partkey#1L, 200)
//                        +- *(9) Project [l_partkey#1L, l_quantity#4, l_extendedprice#5]
//                           +- *(9) Filter isnotnull(l_quantity#4)
//                              +- *(9) FileScan parquet [l_partkey#1L,l_quantity#4,l_extendedprice#5] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/lineitem], PartitionFilters: [], PushedFilters: [IsNotNull(l_quantity)], ReadSchema: struct<l_partkey:bigint,l_quantity:double,l_extendedprice:double>
