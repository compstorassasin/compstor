package tpch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.first
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 18
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q18 extends TpchQuery {

  override def execute(ss: SparkSession, tpchTableProvider: TpchTableProvider, iDirPrefix: String): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    import ss.implicits._
    import tpchTableProvider._

    if (iDirPrefix != "") {
      throw new RuntimeException("shuffle dump not yet implemented")
    }

    lineitem.groupBy($"l_orderkey")
      .agg(sum($"l_quantity").as("sum_quantity"))
      .filter($"sum_quantity" > 300)
      .select($"l_orderkey".as("key"), $"sum_quantity")
      .join(order, order("o_orderkey") === $"key")
      .join(lineitem, $"o_orderkey" === lineitem("l_orderkey"))
      .join(customer, customer("c_custkey") === $"o_custkey")
      .select($"l_quantity", $"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice")
      .groupBy($"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice")
      .agg(sum("l_quantity"))
      .sort($"o_totalprice".desc, $"o_orderdate")
      .limit(100)
  }

}
