package tpch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 22
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q22 extends TpchQuery {

  override def execute(ss: SparkSession, tpchTableProvider: TpchTableProvider, iDirPrefix: String): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    import ss.implicits._
    import tpchTableProvider._

    val sub2 = udf { (x: String) => x.substring(0, 2) }
    val phone = udf { (x: String) => x.matches("13|31|23|29|30|18|17") }
    val isNull = udf { (x: Any) => println(x); true }

    val fcustomer = customer.filter(($"c_phone".startsWith("13")) ||
      (($"c_phone").startsWith("31")) || (($"c_phone").startsWith("23")) ||
      (($"c_phone").startsWith("29")) || (($"c_phone").startsWith("30")) ||
      (($"c_phone").startsWith("18")) || (($"c_phone").startsWith("17"))
    ).select($"c_acctbal", $"c_custkey", sub2($"c_phone").as("cntrycode"))

    if (iDirPrefix != "") {
      throw new RuntimeException("shuffle dump not yet implemented")
    }

    val avg_customer = fcustomer.filter($"c_acctbal" > 0)
      .agg(avg($"c_acctbal").as("avg_acctbal"))

    order.groupBy($"o_custkey")
      .agg($"o_custkey").select($"o_custkey")
      .join(fcustomer, $"o_custkey" === fcustomer("c_custkey"), "right_outer")
      //.filter("o_custkey is null")
      .filter($"o_custkey".isNull)
      .join(avg_customer)
      .filter($"c_acctbal" > $"avg_acctbal")
      .groupBy($"cntrycode")
      .agg(count($"c_acctbal"), sum($"c_acctbal"))
      .sort($"cntrycode")
  }
}
