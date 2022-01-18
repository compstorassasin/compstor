package tpch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 8
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q08 extends TpchQuery {

  override def execute(ss: SparkSession, tpchTableProvider: TpchTableProvider, iDirPrefix: String): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    import ss.implicits._
    import tpchTableProvider._

    val getYear = udf { (x: String) => x.substring(0, 4) }
    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val isBrazil = udf { (x: String, y: Double) => if (x == "BRAZIL") y else 0 }

    val fregion = region.filter($"r_name" === "AMERICA")
    val forder = order.filter($"o_orderdate" <= "1996-12-31" && $"o_orderdate" >= "1995-01-01")
    val fpart = part.filter($"p_type" === "ECONOMY ANODIZED STEEL")

    if (iDirPrefix != "") {
      val fsorder = forder.select("o_custkey", "o_orderkey", "o_orderdate")
      fsorder.write.mode("overwrite").parquet(iDirPrefix + "_fsorder")
      val slineitem = lineitem.select($"l_partkey", $"l_suppkey", $"l_orderkey",
        decrease($"l_extendedprice", $"l_discount").as("volume"))
      slineitem.write.mode("overwrite").parquet(iDirPrefix + "_slineitem")
      val fspart = fpart.select("p_partkey")
      fspart.write.mode("overwrite").parquet(iDirPrefix + "_fspart")
      val nat = nation.join(supplier, $"n_nationkey" === supplier("s_nationkey"))
        .select("s_suppkey", "n_name")
      val line = slineitem.join(fspart, $"l_partkey" === fpart("p_partkey"))
        .join(nat, $"l_suppkey" === nat("s_suppkey"))
        .select("l_orderkey", "volume", "n_name")
      line.write.mode("overwrite").parquet(iDirPrefix + "_line")
      return ss.emptyDataFrame
    }

    val nat = nation.join(supplier, $"n_nationkey" === supplier("s_nationkey"))

    val line = lineitem.select($"l_partkey", $"l_suppkey", $"l_orderkey",
      decrease($"l_extendedprice", $"l_discount").as("volume")).
      join(fpart, $"l_partkey" === fpart("p_partkey"))
      .join(nat, $"l_suppkey" === nat("s_suppkey"))

    nation.join(fregion, $"n_regionkey" === fregion("r_regionkey"))
      .select($"n_nationkey")
      .join(customer, $"n_nationkey" === customer("c_nationkey"))
      .select($"c_custkey")
      .join(forder, $"c_custkey" === forder("o_custkey"))
      .select($"o_orderkey", $"o_orderdate")
      .join(line, $"o_orderkey" === line("l_orderkey"))
      .select(getYear($"o_orderdate").as("o_year"), $"volume",
        isBrazil($"n_name", $"volume").as("case_volume"))
      .groupBy($"o_year")
      .agg(sum($"case_volume") / sum("volume"))
      .sort($"o_year")
  }

}

//== Physical Plan ==
//*(22) Sort [o_year#434 ASC NULLS FIRST], true, 0
//+- Exchange rangepartitioning(o_year#434 ASC NULLS FIRST, 200)
//   +- *(21) HashAggregate(keys=[o_year#434], functions=[sum(case_volume#435), sum(volume#158)], output=[o_year#434, (sum(case_volume) / sum(volume))#444])
//      +- Exchange hashpartitioning(o_year#434, 200)
//         +- *(20) HashAggregate(keys=[o_year#434], functions=[partial_sum(case_volume#435), partial_sum(volume#158)], output=[o_year#434, sum#451, sum#452])
//            +- *(20) Project [UDF(o_orderdate#44) AS o_year#434, volume#158, if (isnull(volume#158)) null else UDF(n_name#33, volume#158) AS case_volume#435]
//               +- *(20) SortMergeJoin [o_orderkey#40L], [l_orderkey#0L], Inner
//                  :- *(8) Sort [o_orderkey#40L ASC NULLS FIRST], false, 0
//                  :  +- Exchange hashpartitioning(o_orderkey#40L, 200)
//                  :     +- *(7) Project [o_orderkey#40L, o_orderdate#44]
//                  :        +- *(7) SortMergeJoin [c_custkey#82L], [o_custkey#41L], Inner
//                  :           :- *(4) Sort [c_custkey#82L ASC NULLS FIRST], false, 0
//                  :           :  +- Exchange hashpartitioning(c_custkey#82L, 200)
//                  :           :     +- *(3) Project [c_custkey#82L]
//                  :           :        +- *(3) BroadcastHashJoin [n_nationkey#32L], [c_nationkey#85L], Inner, BuildLeft
//                  :           :           :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]))
//                  :           :           :  +- *(2) Project [n_nationkey#32L]
//                  :           :           :     +- *(2) BroadcastHashJoin [n_regionkey#34L], [r_regionkey#58L], Inner, BuildRight
//                  :           :           :        :- *(2) Project [n_nationkey#32L, n_regionkey#34L]
//                  :           :           :        :  +- *(2) Filter (isnotnull(n_regionkey#34L) && isnotnull(n_nationkey#32L))
//                  :           :           :        :     +- *(2) FileScan parquet [n_nationkey#32L,n_regionkey#34L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/nation], PartitionFilters: [], PushedFilters: [IsNotNull(n_regionkey), IsNotNull(n_nationkey)], ReadSchema: struct<n_nationkey:bigint,n_regionkey:bigint>
//                  :           :           :        +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]))
//                  :           :           :           +- *(1) Project [r_regionkey#58L]
//                  :           :           :              +- *(1) Filter ((isnotnull(r_name#59) && (r_name#59 = AMERICA)) && isnotnull(r_regionkey#58L))
//                  :           :           :                 +- *(1) FileScan parquet [r_regionkey#58L,r_name#59] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/region], PartitionFilters: [], PushedFilters: [IsNotNull(r_name), EqualTo(r_name,AMERICA), IsNotNull(r_regionkey)], ReadSchema: struct<r_regionkey:bigint,r_name:string>
//                  :           :           +- *(3) Project [c_custkey#82L, c_nationkey#85L]
//                  :           :              +- *(3) Filter (isnotnull(c_nationkey#85L) && isnotnull(c_custkey#82L))
//                  :           :                 +- *(3) FileScan parquet [c_custkey#82L,c_nationkey#85L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/customer], PartitionFilters: [], PushedFilters: [IsNotNull(c_nationkey), IsNotNull(c_custkey)], ReadSchema: struct<c_custkey:bigint,c_nationkey:bigint>
//                  :           +- *(6) Sort [o_custkey#41L ASC NULLS FIRST], false, 0
//                  :              +- Exchange hashpartitioning(o_custkey#41L, 200)
//                  :                 +- *(5) Project [o_orderkey#40L, o_custkey#41L, o_orderdate#44]
//                  :                    +- *(5) Filter ((((isnotnull(o_orderdate#44) && (o_orderdate#44 <= 1996-12-31)) && (o_orderdate#44 >= 1995-01-01)) && isnotnull(o_custkey#41L)) && isnotnull(o_orderkey#40L))
//                  :                       +- *(5) FileScan parquet [o_orderkey#40L,o_custkey#41L,o_orderdate#44] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/orders], PartitionFilters: [], PushedFilters: [IsNotNull(o_orderdate), LessThanOrEqual(o_orderdate,1996-12-31), GreaterThanOrEqual(o_orderdate,1995-01-01), IsNotNull(o_custkey), IsNotNull(o_orderkey)], ReadSchema: struct<o_orderkey:bigint,o_custkey:bigint,o_orderdate:string>
//                  +- *(19) Sort [l_orderkey#0L ASC NULLS FIRST], false, 0
//                     +- Exchange hashpartitioning(l_orderkey#0L, 200)
//                        +- *(18) Project [l_orderkey#0L, volume#158, n_name#33]
//                           +- *(18) SortMergeJoin [l_suppkey#2L], [s_suppkey#98L], Inner
//                              :- *(14) Sort [l_suppkey#2L ASC NULLS FIRST], false, 0
//                              :  +- Exchange hashpartitioning(l_suppkey#2L, 200)
//                              :     +- *(13) Project [l_suppkey#2L, l_orderkey#0L, volume#158]
//                              :        +- *(13) SortMergeJoin [l_partkey#1L], [p_partkey#64L], Inner
//                              :           :- *(10) Sort [l_partkey#1L ASC NULLS FIRST], false, 0
//                              :           :  +- Exchange hashpartitioning(l_partkey#1L, 200)
//                              :           :     +- *(9) Project [l_partkey#1L, l_suppkey#2L, l_orderkey#0L, if ((isnull(l_extendedprice#5) || isnull(l_discount#6))) null else if ((isnull(l_extendedprice#5) || isnull(l_discount#6))) null else if ((isnull(l_extendedprice#5) || isnull(l_discount#6))) null else if ((isnull(l_extendedprice#5) || isnull(l_discount#6))) null else UDF(l_extendedprice#5, l_discount#6) AS volume#158]
//                              :           :        +- *(9) Filter ((isnotnull(l_partkey#1L) && isnotnull(l_suppkey#2L)) && isnotnull(l_orderkey#0L))
//                              :           :           +- *(9) FileScan parquet [l_orderkey#0L,l_partkey#1L,l_suppkey#2L,l_extendedprice#5,l_discount#6] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/lineitem], PartitionFilters: [], PushedFilters: [IsNotNull(l_partkey), IsNotNull(l_suppkey), IsNotNull(l_orderkey)], ReadSchema: struct<l_orderkey:bigint,l_partkey:bigint,l_suppkey:bigint,l_extendedprice:double,l_discount:double>
//                              :           +- *(12) Sort [p_partkey#64L ASC NULLS FIRST], false, 0
//                              :              +- Exchange hashpartitioning(p_partkey#64L, 200)
//                              :                 +- *(11) Project [p_partkey#64L]
//                              :                    +- *(11) Filter ((isnotnull(p_type#68) && (p_type#68 = ECONOMY ANODIZED STEEL)) && isnotnull(p_partkey#64L))
//                              :                       +- *(11) FileScan parquet [p_partkey#64L,p_type#68] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/part], PartitionFilters: [], PushedFilters: [IsNotNull(p_type), EqualTo(p_type,ECONOMY ANODIZED STEEL), IsNotNull(p_partkey)], ReadSchema: struct<p_partkey:bigint,p_type:string>
//                              +- *(17) Sort [s_suppkey#98L ASC NULLS FIRST], false, 0
//                                 +- Exchange hashpartitioning(s_suppkey#98L, 200)
//                                    +- *(16) Project [n_name#33, s_suppkey#98L]
//                                       +- *(16) BroadcastHashJoin [n_nationkey#32L], [s_nationkey#101L], Inner, BuildLeft
//                                          :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]))
//                                          :  +- *(15) Project [n_nationkey#32L, n_name#33]
//                                          :     +- *(15) Filter isnotnull(n_nationkey#32L)
//                                          :        +- *(15) FileScan parquet [n_nationkey#32L,n_name#33] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/nation], PartitionFilters: [], PushedFilters: [IsNotNull(n_nationkey)], ReadSchema: struct<n_nationkey:bigint,n_name:string>
//                                          +- *(16) Project [s_suppkey#98L, s_nationkey#101L]
//                                             +- *(16) Filter (isnotnull(s_nationkey#101L) && isnotnull(s_suppkey#98L))
//                                                +- *(16) FileScan parquet [s_suppkey#98L,s_nationkey#101L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/supplier], PartitionFilters: [], PushedFilters: [IsNotNull(s_nationkey), IsNotNull(s_suppkey)], ReadSchema: struct<s_suppkey:bigint,s_nationkey:bigint>
