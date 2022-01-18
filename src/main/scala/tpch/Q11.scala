package tpch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import java.math.BigDecimal

/**
 * TPC-H Query 11
 * Savvas Savvides <savvas@purdue.edu>
 *``
 */
class Q11 extends TpchQuery {

  override def execute(ss: SparkSession, tpchTableProvider: TpchTableProvider, iDirPrefix: String): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    import ss.implicits._
    import tpchTableProvider._

    if (iDirPrefix != "") {
      val spartsupp = partsupp.select("ps_suppkey", "ps_partkey", "ps_supplycost", "ps_availqty")
      spartsupp.write.mode("overwrite").parquet(iDirPrefix + "_spartsupp")
      return ss.emptyDataFrame
    }

    val tmp = nation.filter($"n_name" === "GERMANY")
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .select($"s_suppkey")
     .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
     .groupBy("ps_partkey").agg(sum($"ps_supplycost" * $"ps_availqty").as("part_value"))
     .select($"ps_partkey", $"part_value")
    // tmp.persist
    // tmp.count

    val total_value = tmp.select("part_value").rdd.map(_(0).asInstanceOf[BigDecimal]).reduce(_.add(_)).scaleByPowerOfTen(-4);

    tmp.filter($"part_value" > total_value).sort($"part_value".desc)
  }

}

//== Physical Plan ==
//*(2) Sort [part_value#182 DESC NULLS LAST], true, 0
//+- Exchange rangepartitioning(part_value#182 DESC NULLS LAST, 200)
//   +- *(1) Filter (isnotnull(part_value#182) && (part_value#182 > 8.005090038809222E9))
//      +- *(1) InMemoryTableScan [ps_partkey#112L, part_value#182], [isnotnull(part_value#182), (part_value#182 > 8.005090038809222E9)]
//            +- InMemoryRelation [ps_partkey#112L, part_value#182], true, 10000, StorageLevel(disk, memory, deserialized, 1 replicas)
//                  +- *(7) HashAggregate(keys=[ps_partkey#112L], functions=[sum  (if ((isnull(ps_supplycost#115) || isnull(cast(ps_availqty#114L as int)))) null else if ((isnull(ps_supplycost#115) || isnull(cast(ps_availqty#114L as int)))) null else UDF(ps_supplycost#115, cast(ps_availqty#114L as int)))], output=[ps_partkey#112L, part_value#182])
//                     +- Exchange hashpartitioning(ps_partkey#112L, 200)
//                        +- *(6) HashAggregate(keys=[ps_partkey#112L], functions=[partial_sum(if ((isnull(ps_supplycost#115) || isnull(cast(ps_availqty#114L as int)))) null else if ((isnull(ps_supplycost#115) || isnull(cast(ps_availqty#114L as int)))) null else UDF(ps_supplycost#115, cast(ps_availqty#114L as int)))], output=[ps_partkey#112L, sum#188])
//                           +- *(6) Project [ps_partkey#112L, ps_availqty#114L, ps_supplycost#115]
//                              +- *(6) SortMergeJoin [s_suppkey#98L], [ps_suppkey#113L], Inner
//                                 :- *(3) Sort [s_suppkey#98L ASC NULLS FIRST], false, 0
//                                 :  +- Exchange hashpartitioning(s_suppkey#98L, 200)
//                                 :     +- *(2) Project [s_suppkey#98L]
//                                 :        +- *(2) BroadcastHashJoin [n_nationkey#32L], [s_nationkey#101L], Inner, BuildLeft
//                                 :           :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]))
//                                 :           :  +- *(1) Project [n_nationkey#32L]
//                                 :           :     +- *(1) Filter ((isnotnull(n_name#33) && (n_name#33 = GERMANY)) && isnotnull(n_nationkey#32L))
//                                 :           :        +- *(1) FileScan parquet [n_nationkey#32L,n_name#33] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/nation], PartitionFilters: [], PushedFilters: [IsNotNull(n_name), EqualTo(n_name,GERMANY), IsNotNull(n_nationkey)], ReadSchema: struct<n_nationkey:bigint,n_name:string>
//                                 :           +- *(2) Project [s_suppkey#98L, s_nationkey#101L]
//                                 :              +- *(2) Filter (isnotnull(s_nationkey#101L) && isnotnull(s_suppkey#98L))
//                                 :                 +- *(2) FileScan parquet [s_suppkey#98L,s_nationkey#101L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/supplier], PartitionFilters: [], PushedFilters: [IsNotNull(s_nationkey), IsNotNull(s_suppkey)], ReadSchema: struct<s_suppkey:bigint,s_nationkey:bigint>
//                                 +- *(5) Sort [ps_suppkey#113L ASC NULLS FIRST], false, 0
//                                    +- Exchange hashpartitioning(ps_suppkey#113L, 200)
//                                       +- *(4) Project [ps_partkey#112L, ps_suppkey#113L, ps_availqty#114L, ps_supplycost#115]
//                                          +- *(4) Filter isnotnull(ps_suppkey#113L)
//                                             +- *(4) FileScan parquet [ps_partkey#112L,ps_suppkey#113L,ps_availqty#114L,ps_supplycost#115] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/partsupp], PartitionFilters: [], PushedFilters: [IsNotNull(ps_suppkey)], ReadSchema: struct<ps_partkey:bigint,ps_suppkey:bigint,ps_availqty:bigint,ps_supplycost:double>
