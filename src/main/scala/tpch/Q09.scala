package tpch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 9
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q09 extends TpchQuery {

  override def execute(ss: SparkSession, tpchTableProvider: TpchTableProvider, iDirPrefix: String): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    import ss.implicits._
    import tpchTableProvider._

    val getYear = udf { (x: String) => x.substring(0, 4) }
    val expr = udf { (x: Double, y: Double, v: Double, w: Double) => x * (1 - y) - (v * w) }

    if (iDirPrefix != "") {
      val slineitem = lineitem.select("l_partkey","l_orderkey", "l_suppkey", "l_quantity", "l_extendedprice", "l_discount")
      slineitem.write.mode("overwrite").parquet(iDirPrefix + "_slineitem")

      val lineitempart = part.filter($"p_name".contains("green"))
        .join(lineitem, $"p_partkey" === lineitem("l_partkey"))
        .select("l_suppkey", "l_partkey","l_orderkey", "l_quantity", "l_extendedprice", "l_discount")
      lineitempart.write.mode("overwrite").parquet(iDirPrefix + "_lineitempart")

      return ss.emptyDataFrame
    }

    val linePart = part.filter($"p_name".contains("green"))
      .join(lineitem, $"p_partkey" === lineitem("l_partkey"))

    val natSup = nation.join(supplier, $"n_nationkey" === supplier("s_nationkey"))

    linePart.join(natSup, $"l_suppkey" === natSup("s_suppkey"))
      .join(partsupp, $"l_suppkey" === partsupp("ps_suppkey")
        && $"l_partkey" === partsupp("ps_partkey"))
      .join(order, $"l_orderkey" === order("o_orderkey"))
      .select($"n_name", getYear($"o_orderdate").as("o_year"),
        expr($"l_extendedprice", $"l_discount", $"ps_supplycost", $"l_quantity").as("amount"))
      .groupBy($"n_name", $"o_year")
      .agg(sum($"amount"))
      .sort($"n_name", $"o_year".desc)

  }

}

//== Physical Plan ==
//*(20) Sort [n_name#33 ASC NULLS FIRST, o_year#612 DESC NULLS LAST], true, 0
//+- Exchange rangepartitioning(n_name#33 ASC NULLS FIRST, o_year#612 DESC NULLS LAST, 200)
//   +- *(19) HashAggregate(keys=[n_name#33, o_year#612], functions=[sum(amount#613)], output=[n_name#33, o_year#612, sum(amount)#621])
//      +- Exchange hashpartitioning(n_name#33, o_year#612, 200)
//         +- *(18) HashAggregate(keys=[n_name#33, o_year#612], functions=[partial_sum(amount#613)], output=[n_name#33, o_year#612, sum#628])
//            +- *(18) Project [n_name#33, UDF(o_orderdate#44) AS o_year#612, if ((((isnull(l_extendedprice#5) || isnull(l_discount#6)) || isnull(ps_supplycost#115)) || isnull(l_quantity#4))) null else UDF(l_extendedprice#5, l_discount#6, ps_supplycost#115, l_quantity#4) AS amount#613]
//               +- *(18) SortMergeJoin [l_orderkey#0L], [o_orderkey#40L], Inner
//                  :- *(15) Sort [l_orderkey#0L ASC NULLS FIRST], false, 0
//                  :  +- Exchange hashpartitioning(l_orderkey#0L, 200)
//                  :     +- *(14) Project [l_orderkey#0L, l_quantity#4, l_extendedprice#5, l_discount#6, n_name#33, ps_supplycost#115]
//                  :        +- *(14) SortMergeJoin [l_suppkey#2L, l_partkey#1L], [ps_suppkey#113L, ps_partkey#112L], Inner
//                  :           :- *(11) Sort [l_suppkey#2L ASC NULLS FIRST, l_partkey#1L ASC NULLS FIRST], false, 0
//                  :           :  +- Exchange hashpartitioning(l_suppkey#2L, l_partkey#1L, 200)
//                  :           :     +- *(10) Project [l_orderkey#0L, l_partkey#1L, l_suppkey#2L, l_quantity#4, l_extendedprice#5, l_discount#6, n_name#33]
//                  :           :        +- *(10) SortMergeJoin [l_suppkey#2L], [s_suppkey#98L], Inner
//                  :           :           :- *(6) Sort [l_suppkey#2L ASC NULLS FIRST], false, 0
//                  :           :           :  +- Exchange hashpartitioning(l_suppkey#2L, 200)
//                  :           :           :     +- *(5) Project [l_orderkey#0L, l_partkey#1L, l_suppkey#2L, l_quantity#4, l_extendedprice#5, l_discount#6]
//                  :           :           :        +- *(5) SortMergeJoin [p_partkey#64L], [l_partkey#1L], Inner
//                  :           :           :           :- *(2) Sort [p_partkey#64L ASC NULLS FIRST], false, 0
//                  :           :           :           :  +- Exchange hashpartitioning(p_partkey#64L, 200)
//                  :           :           :           :     +- *(1) Project [p_partkey#64L]
//                  :           :           :           :        +- *(1) Filter ((isnotnull(p_name#65) && Contains(p_name#65, green)) && isnotnull(p_partkey#64L))
//                  :           :           :           :           +- *(1) FileScan parquet [p_partkey#64L,p_name#65] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/part], PartitionFilters: [], PushedFilters: [IsNotNull(p_name), StringContains(p_name,green), IsNotNull(p_partkey)], ReadSchema: struct<p_partkey:bigint,p_name:string>
//                  :           :           :           +- *(4) Sort [l_partkey#1L ASC NULLS FIRST], false, 0
//                  :           :           :              +- Exchange hashpartitioning(l_partkey#1L, 200)
//                  :           :           :                 +- *(3) Project [l_orderkey#0L, l_partkey#1L, l_suppkey#2L, l_quantity#4, l_extendedprice#5, l_discount#6]
//                  :           :           :                    +- *(3) Filter ((isnotnull(l_partkey#1L) && isnotnull(l_suppkey#2L)) && isnotnull(l_orderkey#0L))
//                  :           :           :                       +- *(3) FileScan parquet [l_orderkey#0L,l_partkey#1L,l_suppkey#2L,l_quantity#4,l_extendedprice#5,l_discount#6] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/lineitem], PartitionFilters: [], PushedFilters: [IsNotNull(l_partkey), IsNotNull(l_suppkey), IsNotNull(l_orderkey)], ReadSchema: struct<l_orderkey:bigint,l_partkey:bigint,l_suppkey:bigint,l_quantity:double,l_extendedprice:double,l_discount:double>
//                  :           :           +- *(9) Sort [s_suppkey#98L ASC NULLS FIRST], false, 0
//                  :           :              +- Exchange hashpartitioning(s_suppkey#98L, 200)
//                  :           :                 +- *(8) Project [n_name#33, s_suppkey#98L]
//                  :           :                    +- *(8) BroadcastHashJoin [n_nationkey#32L], [s_nationkey#101L], Inner, BuildLeft
//                  :           :                       :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]))
//                  :           :                       :  +- *(7) Project [n_nationkey#32L, n_name#33]
//                  :           :                       :     +- *(7) Filter isnotnull(n_nationkey#32L)
//                  :           :                       :        +- *(7) FileScan parquet [n_nationkey#32L,n_name#33] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/nation], PartitionFilters: [], PushedFilters: [IsNotNull(n_nationkey)], ReadSchema: struct<n_nationkey:bigint,n_name:string>
//                  :           :                       +- *(8) Project [s_suppkey#98L, s_nationkey#101L]
//                  :           :                          +- *(8) Filter (isnotnull(s_nationkey#101L) && isnotnull(s_suppkey#98L))
//                  :           :                             +- *(8) FileScan parquet [s_suppkey#98L,s_nationkey#101L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/supplier], PartitionFilters: [], PushedFilters: [IsNotNull(s_nationkey), IsNotNull(s_suppkey)], ReadSchema: struct<s_suppkey:bigint,s_nationkey:bigint>
//                  :           +- *(13) Sort [ps_suppkey#113L ASC NULLS FIRST, ps_partkey#112L ASC NULLS FIRST], false, 0
//                  :              +- Exchange hashpartitioning(ps_suppkey#113L, ps_partkey#112L, 200)
//                  :                 +- *(12) Project [ps_partkey#112L, ps_suppkey#113L, ps_supplycost#115]
//                  :                    +- *(12) Filter (isnotnull(ps_partkey#112L) && isnotnull(ps_suppkey#113L))
//                  :                       +- *(12) FileScan parquet [ps_partkey#112L,ps_suppkey#113L,ps_supplycost#115] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/partsupp], PartitionFilters: [], PushedFilters: [IsNotNull(ps_partkey), IsNotNull(ps_suppkey)], ReadSchema: struct<ps_partkey:bigint,ps_suppkey:bigint,ps_supplycost:double>
//                  +- *(17) Sort [o_orderkey#40L ASC NULLS FIRST], false, 0
//                     +- Exchange hashpartitioning(o_orderkey#40L, 200)
//                        +- *(16) Project [o_orderkey#40L, o_orderdate#44]
//                           +- *(16) Filter isnotnull(o_orderkey#40L)
//                              +- *(16) FileScan parquet [o_orderkey#40L,o_orderdate#44] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/orders], PartitionFilters: [], PushedFilters: [IsNotNull(o_orderkey)], ReadSchema: struct<o_orderkey:bigint,o_orderdate:string>
