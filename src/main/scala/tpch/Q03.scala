package tpch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 3
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q03 extends TpchQuery {

  override def execute(ss: SparkSession, tpchTableProvider: TpchTableProvider, iDirPrefix: String): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    import ss.implicits._
    import tpchTableProvider._

    if (iDirPrefix != "") {
      val fscustomer = customer.filter($"c_mktsegment" === "BUILDING")
        .select("c_custkey", "c_mktsegment")
      fscustomer.write.mode("overwrite").parquet(iDirPrefix + "_fscustomer")
      val fsorder = order.filter($"o_orderdate" < "1995-03-15")
        .select("o_custkey", "o_orderkey", "o_orderdate", "o_shippriority")
      fsorder.write.mode("overwrite").parquet(iDirPrefix + "_fsorder")
      val customer_order = fscustomer.join(fsorder, $"c_custkey" === fsorder("o_custkey"))
        .select("o_orderkey", "o_orderdate", "o_shippriority")
      customer_order.write.mode("overwrite").parquet(iDirPrefix + "_customer_order")
      val fslineitem = lineitem.filter($"l_shipdate" > "1995-03-15")
        .select("l_orderkey", "l_extendedprice", "l_discount", "l_shipdate")
      fslineitem.write.mode("overwrite").parquet(iDirPrefix + "_fslineitem")

      return ss.emptyDataFrame
    }

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val fcust = customer.filter($"c_mktsegment" === "BUILDING")
    val forders = order.filter($"o_orderdate" < "1995-03-15")
    val flineitems = lineitem.filter($"l_shipdate" > "1995-03-15")

    fcust.join(forders, $"c_custkey" === forders("o_custkey"))
      .select($"o_orderkey", $"o_orderdate", $"o_shippriority")
      .join(flineitems, $"o_orderkey" === flineitems("l_orderkey"))
      .select($"l_orderkey",
        decrease($"l_extendedprice", $"l_discount").as("volume"),
        $"o_orderdate", $"o_shippriority")
      .groupBy($"l_orderkey", $"o_orderdate", $"o_shippriority")
      .agg(sum($"volume").as("revenue"))
      .sort($"revenue".desc, $"o_orderdate")
      .limit(10)
  }

}

//== Physical Plan ==
//TakeOrderedAndProject(limit=10, orderBy=[revenue#246 DESC NULLS LAST,o_orderdate#44 ASC NULLS FIRST], output=[l_orderkey#0L,o_orderdate#44,o_shippriority#47L,revenue#246])
//+- *(9) HashAggregate(keys=[l_orderkey#0L, o_orderdate#44, o_shippriority#47L], functions=[sum(volume#236)], output=[l_orderkey#0L, o_orderdate#44, o_shippriority#47L, revenue#246])
//   +- *(9) HashAggregate(keys=[l_orderkey#0L, o_orderdate#44, o_shippriority#47L], functions=[partial_sum(volume#236)], output=[l_orderkey#0L, o_orderdate#44, o_shippriority#47L, sum#255])
//      +- *(9) Project [l_orderkey#0L, if ((isnull(l_extendedprice#5) || isnull(l_discount#6))) null else UDF(l_extendedprice#5, l_discount#6) AS volume#236, o_orderdate#44, o_shippriority#47L]
//         +- *(9) SortMergeJoin [o_orderkey#40L], [l_orderkey#0L], Inner
//            :- *(6) Sort [o_orderkey#40L ASC NULLS FIRST], false, 0
//            :  +- Exchange hashpartitioning(o_orderkey#40L, 200)
//            :     +- *(5) Project [o_orderkey#40L, o_orderdate#44, o_shippriority#47L]
//            :        +- *(5) SortMergeJoin [c_custkey#82L], [o_custkey#41L], Inner
//            :           :- *(2) Sort [c_custkey#82L ASC NULLS FIRST], false, 0
//            :           :  +- Exchange hashpartitioning(c_custkey#82L, 200)
//            :           :     +- *(1) Project [c_custkey#82L]
//            :           :        +- *(1) Filter ((isnotnull(c_mktsegment#88) && (c_mktsegment#88 = BUILDING)) && isnotnull(c_custkey#82L))
//            :           :           +- *(1) FileScan parquet [c_custkey#82L,c_mktsegment#88] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/customer], PartitionFilters: [], PushedFilters: [IsNotNull(c_mktsegment), EqualTo(c_mktsegment,BUILDING), IsNotNull(c_custkey)], ReadSchema: struct<c_custkey:bigint,c_mktsegment:string>
//            :           +- *(4) Sort [o_custkey#41L ASC NULLS FIRST], false, 0
//            :              +- Exchange hashpartitioning(o_custkey#41L, 200)
//            :                 +- *(3) Project [o_orderkey#40L, o_custkey#41L, o_orderdate#44, o_shippriority#47L]
//            :                    +- *(3) Filter (((isnotnull(o_orderdate#44) && (o_orderdate#44 < 1995-03-15)) && isnotnull(o_custkey#41L)) && isnotnull(o_orderkey#40L))
//            :                       +- *(3) FileScan parquet [o_orderkey#40L,o_custkey#41L,o_orderdate#44,o_shippriority#47L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/orders], PartitionFilters: [], PushedFilters: [IsNotNull(o_orderdate), LessThan(o_orderdate,1995-03-15), IsNotNull(o_custkey), IsNotNull(o_orderkey)], ReadSchema: struct<o_orderkey:bigint,o_custkey:bigint,o_orderdate:string,o_shippriority:bigint>
//            +- *(8) Sort [l_orderkey#0L ASC NULLS FIRST], false, 0
//               +- Exchange hashpartitioning(l_orderkey#0L, 200)
//                  +- *(7) Project [l_orderkey#0L, l_extendedprice#5, l_discount#6]
//                     +- *(7) Filter ((isnotnull(l_shipdate#10) && (l_shipdate#10 > 1995-03-15)) && isnotnull(l_orderkey#0L))
//                        +- *(7) FileScan parquet [l_orderkey#0L,l_extendedprice#5,l_discount#6,l_shipdate#10] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/lineitem], PartitionFilters: [], PushedFilters: [IsNotNull(l_shipdate), GreaterThan(l_shipdate,1995-03-15), IsNotNull(l_orderkey)], ReadSchema: struct<l_orderkey:bigint,l_extendedprice:double,l_discount:double,l_shipdate:string>
