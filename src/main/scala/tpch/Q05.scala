package tpch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 5
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q05 extends TpchQuery {

  override def execute(ss: SparkSession, tpchTableProvider: TpchTableProvider, iDirPrefix: String): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    import ss.implicits._
    import tpchTableProvider._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    if (iDirPrefix != "") {
      val supplier_nation_region = region.filter($"r_name" === "ASIA")
        .join(nation, $"r_regionkey" === nation("n_regionkey"))
        .join(supplier, $"n_nationkey" === supplier("s_nationkey")).select("s_suppkey", "s_nationkey", "n_name")
      supplier_nation_region.write.mode("overwrite").parquet(iDirPrefix + "_supplier_nation_region")
      val slineitem = lineitem.select("l_suppkey", "l_orderkey", "l_extendedprice", "l_discount")
      slineitem.write.mode("overwrite").parquet(iDirPrefix + "_slineitem")
      val lineitem_supplier_nation_region = supplier_nation_region.join(slineitem, $"s_suppkey" === lineitem("l_suppkey"))
        .select("l_orderkey", "l_extendedprice", "l_discount", "s_nationkey", "n_name")
      lineitem_supplier_nation_region.write.mode("overwrite").parquet(iDirPrefix + "_slineitemsnr")
      return ss.emptyDataFrame
    }

    val forders = order.filter($"o_orderdate" < "1995-01-01" && $"o_orderdate" >= "1994-01-01")

    region.filter($"r_name" === "ASIA")
      .join(nation, $"r_regionkey" === nation("n_regionkey"))
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .join(lineitem, $"s_suppkey" === lineitem("l_suppkey"))
      .select($"n_name", $"l_extendedprice", $"l_discount", $"l_orderkey", $"s_nationkey")
      .join(forders, $"l_orderkey" === forders("o_orderkey"))
      .join(customer, $"o_custkey" === customer("c_custkey") && $"s_nationkey" === customer("c_nationkey"))
      .select($"n_name", decrease($"l_extendedprice", $"l_discount").as("value"))
      .groupBy($"n_name")
      .agg(sum($"value").as("revenue"))
      .sort($"revenue".desc)
  }

}

//10  1s  43  supplier_nation_region
//11  2.7min  2695  lineitem
//12  17s 675 order
//13  4.6min  200 supplier_nation_region join lineitem
//14  7.3min  124 customer          // Actually low utilizations, waiting for free CPUs

//== Physical Plan ==
//*(17) Sort [revenue#396 DESC NULLS LAST], true, 0
//+- Exchange rangepartitioning(revenue#396 DESC NULLS LAST, 200)
//   +- *(16) HashAggregate(keys=[n_name#33], functions=[sum(value#390)], output=[n_name#33, revenue#396])
//      +- Exchange hashpartitioning(n_name#33, 200)
//         +- *(15) HashAggregate(keys=[n_name#33], functions=[partial_sum(value#390)], output=[n_name#33, sum#402])
//            +- *(15) Project [n_name#33, if ((isnull(l_extendedprice#5) || isnull(l_discount#6))) null else UDF(l_extendedprice#5, l_discount#6) AS value#390]
//               +- *(15) SortMergeJoin [o_custkey#41L, s_nationkey#101L], [c_custkey#82L, c_nationkey#85L], Inner
//                  :- *(12) Sort [o_custkey#41L ASC NULLS FIRST, s_nationkey#101L ASC NULLS FIRST], false, 0
//                  :  +- Exchange hashpartitioning(o_custkey#41L, s_nationkey#101L, 200)
//                  :     +- *(11) Project [n_name#33, l_extendedprice#5, l_discount#6, s_nationkey#101L, o_custkey#41L]
//                  :        +- *(11) SortMergeJoin [l_orderkey#0L], [o_orderkey#40L], Inner
//                  :           :- *(8) Sort [l_orderkey#0L ASC NULLS FIRST], false, 0
//                  :           :  +- Exchange hashpartitioning(l_orderkey#0L, 200)
//                  :           :     +- *(7) Project [n_name#33, l_extendedprice#5, l_discount#6, l_orderkey#0L, s_nationkey#101L]
//                  :           :        +- *(7) SortMergeJoin [s_suppkey#98L], [l_suppkey#2L], Inner
//                  :           :           :- *(4) Sort [s_suppkey#98L ASC NULLS FIRST], false, 0
//                  :           :           :  +- Exchange hashpartitioning(s_suppkey#98L, 200)
//                  :           :           :     +- *(3) Project [n_name#33, s_suppkey#98L, s_nationkey#101L]
//                  :           :           :        +- *(3) BroadcastHashJoin [n_nationkey#32L], [s_nationkey#101L], Inner, BuildLeft
//                  :           :           :           :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]))
//                  :           :           :           :  +- *(2) Project [n_nationkey#32L, n_name#33]
//                  :           :           :           :     +- *(2) BroadcastHashJoin [r_regionkey#58L], [n_regionkey#34L], Inner, BuildLeft
//                  :           :           :           :        :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]))
//                  :           :           :           :        :  +- *(1) Project [r_regionkey#58L]
//                  :           :           :           :        :     +- *(1) Filter ((isnotnull(r_name#59) && (r_name#59 = ASIA)) && isnotnull(r_regionkey#58L))
//                  :           :           :           :        :        +- *(1) FileScan parquet [r_regionkey#58L,r_name#59] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/region], PartitionFilters: [], PushedFilters: [IsNotNull(r_name), EqualTo(r_name,ASIA), IsNotNull(r_regionkey)], ReadSchema: struct<r_regionkey:bigint,r_name:string>
//                  :           :           :           :        +- *(2) Project [n_nationkey#32L, n_name#33, n_regionkey#34L]
//                  :           :           :           :           +- *(2) Filter (isnotnull(n_regionkey#34L) && isnotnull(n_nationkey#32L))
//                  :           :           :           :              +- *(2) FileScan parquet [n_nationkey#32L,n_name#33,n_regionkey#34L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/nation], PartitionFilters: [], PushedFilters: [IsNotNull(n_regionkey), IsNotNull(n_nationkey)], ReadSchema: struct<n_nationkey:bigint,n_name:string,n_regionkey:bigint>
//                  :           :           :           +- *(3) Project [s_suppkey#98L, s_nationkey#101L]
//                  :           :           :              +- *(3) Filter (isnotnull(s_nationkey#101L) && isnotnull(s_suppkey#98L))
//                  :           :           :                 +- *(3) FileScan parquet [s_suppkey#98L,s_nationkey#101L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/supplier], PartitionFilters: [], PushedFilters: [IsNotNull(s_nationkey), IsNotNull(s_suppkey)], ReadSchema: struct<s_suppkey:bigint,s_nationkey:bigint>
//                  :           :           +- *(6) Sort [l_suppkey#2L ASC NULLS FIRST], false, 0
//                  :           :              +- Exchange hashpartitioning(l_suppkey#2L, 200)
//                  :           :                 +- *(5) Project [l_orderkey#0L, l_suppkey#2L, l_extendedprice#5, l_discount#6]
//                  :           :                    +- *(5) Filter (isnotnull(l_suppkey#2L) && isnotnull(l_orderkey#0L))
//                  :           :                       +- *(5) FileScan parquet [l_orderkey#0L,l_suppkey#2L,l_extendedprice#5,l_discount#6] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/lineitem], PartitionFilters: [], PushedFilters: [IsNotNull(l_suppkey), IsNotNull(l_orderkey)], ReadSchema: struct<l_orderkey:bigint,l_suppkey:bigint,l_extendedprice:double,l_discount:double>
//                  :           +- *(10) Sort [o_orderkey#40L ASC NULLS FIRST], false, 0
//                  :              +- Exchange hashpartitioning(o_orderkey#40L, 200)
//                  :                 +- *(9) Project [o_orderkey#40L, o_custkey#41L]
//                  :                    +- *(9) Filter ((((isnotnull(o_orderdate#44) && (o_orderdate#44 < 1995-01-01)) && (o_orderdate#44 >= 1994-01-01)) && isnotnull(o_orderkey#40L)) && isnotnull(o_custkey#41L))
//                  :                       +- *(9) FileScan parquet [o_orderkey#40L,o_custkey#41L,o_orderdate#44] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/orders], PartitionFilters: [], PushedFilters: [IsNotNull(o_orderdate), LessThan(o_orderdate,1995-01-01), GreaterThanOrEqual(o_orderdate,1994-01-01), IsNotNull(o_orderkey), IsNotNull(o_custkey)], ReadSchema: struct<o_orderkey:bigint,o_custkey:bigint,o_orderdate:string>
//                  +- *(14) Sort [c_custkey#82L ASC NULLS FIRST, c_nationkey#85L ASC NULLS FIRST], false, 0
//                     +- Exchange hashpartitioning(c_custkey#82L, c_nationkey#85L, 200)
//                        +- *(13) Project [c_custkey#82L, c_nationkey#85L]
//                           +- *(13) Filter (isnotnull(c_nationkey#85L) && isnotnull(c_custkey#82L))
//                              +- *(13) FileScan parquet [c_custkey#82L,c_nationkey#85L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/customer], PartitionFilters: [], PushedFilters: [IsNotNull(c_nationkey), IsNotNull(c_custkey)], ReadSchema: struct<c_custkey:bigint,c_nationkey:bigint>
