package tpch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 13
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q13 extends TpchQuery {

  override def execute(ss: SparkSession, tpchTableProvider: TpchTableProvider, iDirPrefix: String): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    import ss.implicits._
    import tpchTableProvider._

    val special = udf { (x: String) => x.matches(".*special.*requests.*") }

    if (iDirPrefix != "") {
      throw new RuntimeException("shuffle dump not yet implemented")
    }

    customer.join(order, $"c_custkey" === order("o_custkey")
      && !special(order("o_comment")), "left_outer")
      .groupBy($"o_custkey")
      .agg(count($"o_orderkey").as("c_count"))
      .groupBy($"c_count")
      .agg(count($"o_custkey").as("custdist"))
      .sort($"custdist".desc, $"c_count".desc)
  }

}
