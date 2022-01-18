package tpch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
  * TPC-H Query 12
  * Savvas Savvides <savvas@purdue.edu>
  *
  */
class Q12 extends TpchQuery {

  override def execute(ss: SparkSession, tpchTableProvider: TpchTableProvider, iDirPrefix: String): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    import ss.implicits._
    import tpchTableProvider._

    val mul = udf { (x: Double, y: Double) => x * y }
    val highPriority = udf { (x: String) => if (x == "1-URGENT" || x == "2-HIGH") 1 else 0 }
    val lowPriority = udf { (x: String) => if (x != "1-URGENT" && x != "2-HIGH") 1 else 0 }

    if (iDirPrefix != "") {
      val flineitem = lineitem.filter((
        $"l_shipmode" === "MAIL" || $"l_shipmode" === "SHIP") &&
        $"l_commitdate" < $"l_receiptdate" &&
        $"l_shipdate" < $"l_commitdate" &&
        $"l_receiptdate" >= "1994-01-01" && $"l_receiptdate" < "1995-01-01")
      val fslineitem = flineitem.select($"l_orderkey", $"l_shipmode")
      val sorder = order.select($"o_orderkey", $"o_orderpriority")

      fslineitem.write.mode("overwrite").parquet(iDirPrefix + "_fslineitem")
      sorder.write.mode("overwrite").parquet(iDirPrefix + "_sorder")
      return ss.emptyDataFrame
    }
    lineitem.filter((
      $"l_shipmode" === "MAIL" || $"l_shipmode" === "SHIP") &&
      $"l_commitdate" < $"l_receiptdate" &&
      $"l_shipdate" < $"l_commitdate" &&
      $"l_receiptdate" >= "1994-01-01" && $"l_receiptdate" < "1995-01-01")
      .join(order, $"l_orderkey" === order("o_orderkey"))
      .select($"l_shipmode", $"o_orderpriority")
      .groupBy($"l_shipmode")
      .agg(sum(highPriority($"o_orderpriority")).as("sum_highorderpriority"),
        sum(lowPriority($"o_orderpriority")).as("sum_loworderpriority"))
      .sort($"l_shipmode")
  }
}
