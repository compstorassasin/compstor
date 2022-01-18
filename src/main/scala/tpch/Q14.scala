package tpch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 14
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q14 extends TpchQuery {

  override def execute(ss: SparkSession, tpchTableProvider: TpchTableProvider, iDirPrefix: String): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    import ss.implicits._
    import tpchTableProvider._

    val reduce = udf { (x: Double, y: Double) => x * (1 - y) }
    val promo = udf { (x: String, y: Double) => if (x.startsWith("PROMO")) y else 0 }

    if (iDirPrefix != "") {
      val spart = part.select($"p_partkey", $"p_type")
      val fslineitem = lineitem.filter($"l_shipdate" >= "1995-09-01" && $"l_shipdate" < "1995-10-01").select($"l_partkey", $"l_extendedprice", $"l_discount")
      spart.write.mode("overwrite").parquet(iDirPrefix + "_spart")
      fslineitem.write.mode("overwrite").parquet(iDirPrefix + "_fslineitem")
      return ss.emptyDataFrame
    }

    part.join(lineitem, $"l_partkey" === $"p_partkey" &&
      $"l_shipdate" >= "1995-09-01" && $"l_shipdate" < "1995-10-01")
      .select($"p_type", reduce($"l_extendedprice", $"l_discount").as("value"))
      .agg(sum(promo($"p_type", $"value")) * 100 / sum($"value"))

  }

}
