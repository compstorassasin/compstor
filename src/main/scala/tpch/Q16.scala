package tpch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 16
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q16 extends TpchQuery {

  override def execute(ss: SparkSession, tpchTableProvider: TpchTableProvider, iDirPrefix: String): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    import ss.implicits._
    import tpchTableProvider._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val complains = udf { (x: String) => x.matches(".*Customer.*Complaints.*") }

    if (iDirPrefix != "") {
      val spartsupp = partsupp.select("ps_partkey", "ps_suppkey")
      spartsupp.write.mode("overwrite").parquet(iDirPrefix + "_spartsupp")
      val fspart = part.filter(($"p_brand" =!= "Brand#45") && (!($"p_type").startsWith("MEDIUM POLISHED")) && (
        ($"p_size" === 3) || ($"p_size" === 9) || ($"p_size" === 14) || ($"p_size" === 19) || ($"p_size" === 23) || ($"p_size" === 36) || ($"p_size" === 45) || ($"p_size" === 49)))
        .select($"p_partkey", $"p_brand", $"p_type", $"p_size")
      fspart.write.mode("overwrite").parquet(iDirPrefix + "_fspart")
      val supplier_partsupp = supplier.filter(!complains($"s_comment"))
        .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
        .select($"ps_partkey", $"ps_suppkey")
      supplier_partsupp.write.mode("overwrite").parquet(iDirPrefix + "_supplier_partsupp")
      val supplier_partsupp_part = supplier_partsupp.join(fspart, $"ps_partkey" === fspart("p_partkey"))
        .select("ps_suppkey", "p_brand", "p_type", "p_size")
      supplier_partsupp_part.write.mode("overwrite").parquet(iDirPrefix + "_supplier_partsupp_part")
      return ss.emptyDataFrame
    }

    val fparts = part.filter(($"p_brand" =!= "Brand#45") && (!($"p_type").startsWith("MEDIUM POLISHED")) && (
      ($"p_size" === 3) || ($"p_size" === 9) || ($"p_size" === 14) || ($"p_size" === 19) || ($"p_size" === 23) || ($"p_size" === 36) || ($"p_size" === 45) || ($"p_size" === 49)))
      .select($"p_partkey", $"p_brand", $"p_type", $"p_size")

    supplier.filter(!complains($"s_comment"))
      // .select($"s_suppkey")
      .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
      .select($"ps_partkey", $"ps_suppkey")
      .join(fparts, $"ps_partkey" === fparts("p_partkey"))
      .groupBy($"p_brand", $"p_type", $"p_size")
      .agg(countDistinct($"ps_suppkey").as("supplier_count"))
      .sort($"supplier_count".desc, $"p_brand", $"p_type", $"p_size")
  }

}

//== Physical Plan ==
//*(12) Sort [supplier_count#191L DESC NULLS LAST, p_brand#67 ASC NULLS FIRST, p_type#68 ASC NULLS FIRST, p_size#69L ASC NULLS FIRST], true, 0
//+- Exchange rangepartitioning(supplier_count#191L DESC NULLS LAST, p_brand#67 ASC NULLS FIRST, p_type#68 ASC NULLS FIRST, p_size#69L ASC NULLS FIRST, 200)
//   +- *(11) HashAggregate(keys=[p_brand#67, p_type#68, p_size#69L], functions=[count(distinct ps_suppkey#113L)], output=[p_brand#67, p_type#68, p_size#69L, supplier_count#191L])
//      +- Exchange hashpartitioning(p_brand#67, p_type#68, p_size#69L, 200)
//         +- *(10) HashAggregate(keys=[p_brand#67, p_type#68, p_size#69L], functions=[partial_count(distinct ps_suppkey#113L)], output=[p_brand#67, p_type#68, p_size#69L, count#200L])
//            +- *(10) HashAggregate(keys=[p_brand#67, p_type#68, p_size#69L, ps_suppkey#113L], functions=[], output=[p_brand#67, p_type#68, p_size#69L, ps_suppkey#113L])
//               +- Exchange hashpartitioning(p_brand#67, p_type#68, p_size#69L, ps_suppkey#113L, 200)
//                  +- *(9) HashAggregate(keys=[p_brand#67, p_type#68, p_size#69L, ps_suppkey#113L], functions=[], output=[p_brand#67, p_type#68, p_size#69L, ps_suppkey#113L])
//                     +- *(9) Project [ps_suppkey#113L, p_brand#67, p_type#68, p_size#69L]
//                        +- *(9) SortMergeJoin [ps_partkey#112L], [p_partkey#64L], Inner
//                           :- *(6) Sort [ps_partkey#112L ASC NULLS FIRST], false, 0
//                           :  +- Exchange hashpartitioning(ps_partkey#112L, 200)
//                           :     +- *(5) Project [ps_partkey#112L, ps_suppkey#113L]
//                           :        +- *(5) SortMergeJoin [s_suppkey#98L], [ps_suppkey#113L], Inner
//                           :           :- *(2) Sort [s_suppkey#98L ASC NULLS FIRST], false, 0
//                           :           :  +- Exchange hashpartitioning(s_suppkey#98L, 200)
//                           :           :     +- *(1) Project [s_suppkey#98L]
//                           :           :        +- *(1) Filter (NOT UDF(s_comment#104) && isnotnull(s_suppkey#98L))
//                           :           :           +- *(1) FileScan parquet [s_suppkey#98L,s_comment#104] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/supplier], PartitionFilters: [], PushedFilters: [IsNotNull(s_suppkey)], ReadSchema: struct<s_suppkey:bigint,s_comment:string>
//                           :           +- *(4) Sort [ps_suppkey#113L ASC NULLS FIRST], false, 0
//                           :              +- Exchange hashpartitioning(ps_suppkey#113L, 200)
//                           :                 +- *(3) Project [ps_partkey#112L, ps_suppkey#113L]
//                           :                    +- *(3) Filter (isnotnull(ps_suppkey#113L) && isnotnull(ps_partkey#112L))
//                           :                       +- *(3) FileScan parquet [ps_partkey#112L,ps_suppkey#113L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/partsupp], PartitionFilters: [], PushedFilters: [IsNotNull(ps_suppkey), IsNotNull(ps_partkey)], ReadSchema: struct<ps_partkey:bigint,ps_suppkey:bigint>
//                           +- *(8) Sort [p_partkey#64L ASC NULLS FIRST], false, 0
//                              +- Exchange hashpartitioning(p_partkey#64L, 200)
//                                 +- *(7) Project [p_partkey#64L, p_brand#67, p_type#68, p_size#69L]
//                                    +- *(7) Filter ((((isnotnull(p_brand#67) && NOT (p_brand#67 = Brand#45)) && NOT UDF(p_type#68)) && if (isnull(cast(p_size#69L as int))) null else if (isnull(cast(p_size#69L as int))) null else UDF(cast(p_size#69L as int))) && isnotnull(p_partkey#64L))
//                                       +- *(7) FileScan parquet [p_partkey#64L,p_brand#67,p_type#68,p_size#69L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/colouser51/cstorage/tpch/parquet_s1e3_bypass/part], PartitionFilters: [], PushedFilters: [IsNotNull(p_brand), Not(EqualTo(p_brand,Brand#45)), IsNotNull(p_partkey)], ReadSchema: struct<p_partkey:bigint,p_brand:string,p_type:string,p_size:bigint>
