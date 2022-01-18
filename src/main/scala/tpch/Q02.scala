package tpch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.min

/**
 * TPC-H Query 2
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q02 extends TpchQuery {

  override def execute(ss: SparkSession, tpchTableProvider: TpchTableProvider, iDirPrefix: String): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    import ss.implicits._
    import tpchTableProvider._

    if (iDirPrefix != "") {
      val ssupplier = supplier.select("s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment")

      val spartsupp = partsupp.select("ps_suppkey", "ps_partkey", "ps_supplycost")
      val seurope = region.filter($"r_name" === "EUROPE")
        .join(nation, $"r_regionkey" === nation("n_regionkey"))
        .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
        .join(partsupp, supplier("s_suppkey") === partsupp("ps_suppkey"))
        .select($"ps_partkey", $"ps_supplycost", $"s_acctbal", $"s_name", $"n_name", $"s_address", $"s_phone")

      ssupplier.write.mode("overwrite").parquet(iDirPrefix + "_ssupplier")
      spartsupp.write.mode("overwrite").parquet(iDirPrefix + "_spartsupp")
      seurope.write.mode("overwrite").parquet(iDirPrefix + "_seurope")
      return ss.emptyDataFrame
    }

    val europe = region.filter($"r_name" === "EUROPE")
      .join(nation, $"r_regionkey" === nation("n_regionkey"))
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .join(partsupp, supplier("s_suppkey") === partsupp("ps_suppkey"))
//      .select($"ps_partkey", $"ps_supplycost", $"s_acctbal", $"s_name", $"n_name", $"s_address", $"s_phone", $"s_comment")

    val brass = part.filter(part("p_size") === 15 && part("p_type").endsWith("BRASS"))
      .join(europe, europe("ps_partkey") === $"p_partkey")
      .select($"ps_partkey", $"ps_supplycost", $"s_acctbal", $"s_name", $"n_name", $"p_partkey", $"p_mfgr", $"s_address", $"s_phone", $"s_comment")
    brass.persist()
    brass.count()

    val minCost = brass.groupBy(brass("ps_partkey"))
      .agg(min("ps_supplycost").as("min"))

    brass.join(minCost, brass("ps_partkey") === minCost("ps_partkey"))
      .filter(brass("ps_supplycost") === minCost("min"))
      .select("s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment")
      .sort($"s_acctbal".desc, $"n_name", $"s_name", $"p_partkey")
      .limit(100)
  }

}

//28 s  200/200 26.9 GB 4.2 GB  partsupp x supplier
//22 s  461/461 7.8 GB  11.8 GB partsupp
//8 s  43/43 763.1 MB  218 MB supplier

//== Physical Plan ==
//TakeOrderedAndProject(limit=100, orderBy=[s_acctbal#103 DESC NULLS LAST,n_name#33 ASC NULLS FIRST,s_name#99 ASC NULLS FIRST,p_partkey#64L ASC NULLS FIRST], output=[s_acctbal#103,s_name#99,n_name#33,p_partkey#64L,p_mfgr#66,s_address#100,s_phone#102,s_comment#104])
//+- *(6) Project [s_acctbal#103, s_name#99, n_name#33, p_partkey#64L, p_mfgr#66, s_address#100, s_phone#102, s_comment#104]
//   +- *(6) SortMergeJoin [ps_supplycost#115, ps_partkey#112L], [min#464, ps_partkey#467L], Inner
//      :- *(2) Sort [ps_supplycost#115 ASC NULLS FIRST, ps_partkey#112L ASC NULLS FIRST], false, 0
//      :  +- Exchange hashpartitioning(ps_supplycost#115, ps_partkey#112L, 200)
//      :     +- *(1) Filter (isnotnull(ps_supplycost#115) && isnotnull(ps_partkey#112L))
//      :        +- InMemoryTableScan [ps_partkey#112L, ps_supplycost#115, s_acctbal#103, s_name#99, n_name#33, p_partkey#64L, p_mfgr#66, s_address#100, s_phone#102, s_comment#104], [isnotnull(ps_supplycost#115), isnotnull(ps_partkey#112L)]
//      :              +- InMemoryRelation [ps_partkey#112L, ps_supplycost#115, s_acctbal#103, s_name#99, n_name#33, p_partkey#64L, p_mfgr#66, s_address#100, s_phone#102, s_comment#104], true, 10000, StorageLevel(disk, memory, deserialized, 1 replicas)
//      :                    +- *(11) Project [ps_partkey#112L, ps_supplycost#115, s_acctbal#103, s_name#99, n_name#33, p_partkey#64L, p_mfgr#66, s_address#100, s_phone#102, s_comment#104]
//      :                       +- *(11) SortMergeJoin [p_partkey#64L], [ps_partkey#112L], Inner
//      :                          :- *(2) Sort [p_partkey#64L ASC NULLS FIRST], false, 0
//      :                          :  +- Exchange hashpartitioning(p_partkey#64L, 200)
//      :                          :     +- *(1) Project [p_partkey#64L, p_mfgr#66]
//      :                          :        +- *(1) Filter ((((isnotnull(p_size#69L) && isnotnull(p_type#68)) && (p_size#69L = 15)) && EndsWith(p_type#68, BRASS)) && isnotnull(p_partkey#64L))
//      :                          :           +- *(1) FileScan parquet [p_partkey#64L,p_mfgr#66,p_type#68,p_size#69L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/part], PartitionFilters: [], PushedFilters: [IsNotNull(p_size), IsNotNull(p_type), EqualTo(p_size,15), StringEndsWith(p_type,BRASS), IsNotNull(p_partkey)], ReadSchema: struct<p_partkey:bigint,p_mfgr:string,p_type:string,p_size:bigint>
//      :                          +- *(10) Sort [ps_partkey#112L ASC NULLS FIRST], false, 0
//      :                             +- Exchange hashpartitioning(ps_partkey#112L, 200)
//      :                                +- *(9) Project [n_name#33, s_name#99, s_address#100, s_phone#102, s_acctbal#103, s_comment#104, ps_partkey#112L, ps_supplycost#115]
//      :                                   +- *(9) SortMergeJoin [s_suppkey#98L], [ps_suppkey#113L], Inner
//      :                                      :- *(6) Sort [s_suppkey#98L ASC NULLS FIRST], false, 0
//      :                                      :  +- Exchange hashpartitioning(s_suppkey#98L, 200)
//      :                                      :     +- *(5) Project [n_name#33, s_suppkey#98L, s_name#99, s_address#100, s_phone#102, s_acctbal#103, s_comment#104]
//      :                                      :        +- *(5) BroadcastHashJoin [n_nationkey#32L], [s_nationkey#101L], Inner, BuildLeft
//      :                                      :           :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]))
//      :                                      :           :  +- *(4) Project [n_nationkey#32L, n_name#33]
//      :                                      :           :     +- *(4) BroadcastHashJoin [r_regionkey#58L], [n_regionkey#34L], Inner, BuildLeft
//      :                                      :           :        :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]))
//      :                                      :           :        :  +- *(3) Project [r_regionkey#58L]
//      :                                      :           :        :     +- *(3) Filter ((isnotnull(r_name#59) && (r_name#59 = EUROPE)) && isnotnull(r_regionkey#58L))
//      :                                      :           :        :        +- *(3) FileScan parquet [r_regionkey#58L,r_name#59] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/region], PartitionFilters: [], PushedFilters: [IsNotNull(r_name), EqualTo(r_name,EUROPE), IsNotNull(r_regionkey)], ReadSchema: struct<r_regionkey:bigint,r_name:string>
//      :                                      :           :        +- *(4) Project [n_nationkey#32L, n_name#33, n_regionkey#34L]
//      :                                      :           :           +- *(4) Filter (isnotnull(n_regionkey#34L) && isnotnull(n_nationkey#32L))
//      :                                      :           :              +- *(4) FileScan parquet [n_nationkey#32L,n_name#33,n_regionkey#34L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/nation], PartitionFilters: [], PushedFilters: [IsNotNull(n_regionkey), IsNotNull(n_nationkey)], ReadSchema: struct<n_nationkey:bigint,n_name:string,n_regionkey:bigint>
//      :                                      :           +- *(5) Project [s_suppkey#98L, s_name#99, s_address#100, s_nationkey#101L, s_phone#102, s_acctbal#103, s_comment#104]
//      :                                      :              +- *(5) Filter (isnotnull(s_nationkey#101L) && isnotnull(s_suppkey#98L))
//      :                                      :                 +- *(5) FileScan parquet [s_suppkey#98L,s_name#99,s_address#100,s_nationkey#101L,s_phone#102,s_acctbal#103,s_comment#104] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/supplier], PartitionFilters: [], PushedFilters: [IsNotNull(s_nationkey), IsNotNull(s_suppkey)], ReadSchema: struct<s_suppkey:bigint,s_name:string,s_address:string,s_nationkey:bigint,s_phone:string,s_acctbal:double,s_comment:string>
//      :                                      +- *(8) Sort [ps_suppkey#113L ASC NULLS FIRST], false, 0
//      :                                         +- Exchange hashpartitioning(ps_suppkey#113L, 200)
//      :                                            +- *(7) Project [ps_partkey#112L, ps_suppkey#113L, ps_supplycost#115]
//      :                                               +- *(7) Filter (isnotnull(ps_suppkey#113L) && isnotnull(ps_partkey#112L))
//      :                                                  +- *(7) FileScan parquet [ps_partkey#112L,ps_suppkey#113L,ps_supplycost#115] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/partsupp], PartitionFilters: [], PushedFilters: [IsNotNull(ps_suppkey), IsNotNull(ps_partkey)], ReadSchema: struct<ps_partkey:bigint,ps_suppkey:bigint,ps_supplycost:double>
//      +- *(5) Sort [min#464 ASC NULLS FIRST, ps_partkey#467L ASC NULLS FIRST], false, 0
//         +- Exchange hashpartitioning(min#464, ps_partkey#467L, 200)
//            +- *(4) Filter isnotnull(min#464)
//               +- *(4) HashAggregate(keys=[ps_partkey#467L], functions=[min(ps_supplycost#470)], output=[ps_partkey#467L, min#464])
//                  +- Exchange hashpartitioning(ps_partkey#467L, 200)
//                     +- *(3) HashAggregate(keys=[ps_partkey#467L], functions=[partial_min(ps_supplycost#470)], output=[ps_partkey#467L, min#634])
//                        +- *(3) Filter isnotnull(ps_partkey#467L)
//                           +- InMemoryTableScan [ps_partkey#467L, ps_supplycost#470], [isnotnull(ps_partkey#467L)]
//                                 +- InMemoryRelation [ps_partkey#467L, ps_supplycost#470, s_acctbal#103, s_name#99, n_name#33, p_partkey#64L, p_mfgr#66, s_address#100, s_phone#102, s_comment#104], true, 10000, StorageLevel(disk, memory, deserialized, 1 replicas)
//                                       +- *(11) Project [ps_partkey#112L, ps_supplycost#115, s_acctbal#103, s_name#99, n_name#33, p_partkey#64L, p_mfgr#66, s_address#100, s_phone#102, s_comment#104]
//                                          +- *(11) SortMergeJoin [p_partkey#64L], [ps_partkey#112L], Inner
//                                             :- *(2) Sort [p_partkey#64L ASC NULLS FIRST], false, 0
//                                             :  +- Exchange hashpartitioning(p_partkey#64L, 200)
//                                             :     +- *(1) Project [p_partkey#64L, p_mfgr#66]
//                                             :        +- *(1) Filter ((((isnotnull(p_size#69L) && isnotnull(p_type#68)) && (p_size#69L = 15)) && EndsWith(p_type#68, BRASS)) && isnotnull(p_partkey#64L))
//                                             :           +- *(1) FileScan parquet [p_partkey#64L,p_mfgr#66,p_type#68,p_size#69L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/part], PartitionFilters: [], PushedFilters: [IsNotNull(p_size), IsNotNull(p_type), EqualTo(p_size,15), StringEndsWith(p_type,BRASS), IsNotNull(p_partkey)], ReadSchema: struct<p_partkey:bigint,p_mfgr:string,p_type:string,p_size:bigint>
//                                             +- *(10) Sort [ps_partkey#112L ASC NULLS FIRST], false, 0
//                                                +- Exchange hashpartitioning(ps_partkey#112L, 200)
//                                                   +- *(9) Project [n_name#33, s_name#99, s_address#100, s_phone#102, s_acctbal#103, s_comment#104, ps_partkey#112L, ps_supplycost#115]
//                                                      +- *(9) SortMergeJoin [s_suppkey#98L], [ps_suppkey#113L], Inner
//                                                         :- *(6) Sort [s_suppkey#98L ASC NULLS FIRST], false, 0
//                                                         :  +- Exchange hashpartitioning(s_suppkey#98L, 200)
//                                                         :     +- *(5) Project [n_name#33, s_suppkey#98L, s_name#99, s_address#100, s_phone#102, s_acctbal#103, s_comment#104]
//                                                         :        +- *(5) BroadcastHashJoin [n_nationkey#32L], [s_nationkey#101L], Inner, BuildLeft
//                                                         :           :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]))
//                                                         :           :  +- *(4) Project [n_nationkey#32L, n_name#33]
//                                                         :           :     +- *(4) BroadcastHashJoin [r_regionkey#58L], [n_regionkey#34L], Inner, BuildLeft
//                                                         :           :        :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]))
//                                                         :           :        :  +- *(3) Project [r_regionkey#58L]
//                                                         :           :        :     +- *(3) Filter ((isnotnull(r_name#59) && (r_name#59 = EUROPE)) && isnotnull(r_regionkey#58L))
//                                                         :           :        :        +- *(3) FileScan parquet [r_regionkey#58L,r_name#59] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/region], PartitionFilters: [], PushedFilters: [IsNotNull(r_name), EqualTo(r_name,EUROPE), IsNotNull(r_regionkey)], ReadSchema: struct<r_regionkey:bigint,r_name:string>
//                                                         :           :        +- *(4) Project [n_nationkey#32L, n_name#33, n_regionkey#34L]
//                                                         :           :           +- *(4) Filter (isnotnull(n_regionkey#34L) && isnotnull(n_nationkey#32L))
//                                                         :           :              +- *(4) FileScan parquet [n_nationkey#32L,n_name#33,n_regionkey#34L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/nation], PartitionFilters: [], PushedFilters: [IsNotNull(n_regionkey), IsNotNull(n_nationkey)], ReadSchema: struct<n_nationkey:bigint,n_name:string,n_regionkey:bigint>
//                                                         :           +- *(5) Project [s_suppkey#98L, s_name#99, s_address#100, s_nationkey#101L, s_phone#102, s_acctbal#103, s_comment#104]
//                                                         :              +- *(5) Filter (isnotnull(s_nationkey#101L) && isnotnull(s_suppkey#98L))
//                                                         :                 +- *(5) FileScan parquet [s_suppkey#98L,s_name#99,s_address#100,s_nationkey#101L,s_phone#102,s_acctbal#103,s_comment#104] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/supplier], PartitionFilters: [], PushedFilters: [IsNotNull(s_nationkey), IsNotNull(s_suppkey)], ReadSchema: struct<s_suppkey:bigint,s_name:string,s_address:string,s_nationkey:bigint,s_phone:string,s_acctbal:double,s_comment:string>
//                                                         +- *(8) Sort [ps_suppkey#113L ASC NULLS FIRST], false, 0
//                                                            +- Exchange hashpartitioning(ps_suppkey#113L, 200)
//                                                               +- *(7) Project [ps_partkey#112L, ps_suppkey#113L, ps_supplycost#115]
//                                                                  +- *(7) Filter (isnotnull(ps_suppkey#113L) && isnotnull(ps_partkey#112L))
//                                                                     +- *(7) FileScan parquet [ps_partkey#112L,ps_suppkey#113L,ps_supplycost#115] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/partsupp], PartitionFilters: [], PushedFilters: [IsNotNull(ps_suppkey), IsNotNull(ps_partkey)], ReadSchema: struct<ps_partkey:bigint,ps_suppkey:bigint,ps_supplycost:double>
