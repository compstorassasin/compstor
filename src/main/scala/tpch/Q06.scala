package tpch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types.Decimal


/**
 * TPC-H Query 6
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q06 extends TpchQuery {

  override def execute(ss: SparkSession, tpchTableProvider: TpchTableProvider, iDirPrefix: String): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    import ss.implicits._
    import tpchTableProvider._

    if (iDirPrefix != "") {
      throw new RuntimeException("shuffle dump not yet implemented")
    }

    lineitem.filter($"l_shipdate" >= "1994-01-01" && $"l_shipdate" < "1995-01-01" && $"l_discount" >= Decimal.apply(5, 12, 2) && $"l_discount" <= Decimal.apply(7, 12, 2) && $"l_quantity" < 24)
      .agg(sum($"l_extendedprice" * $"l_discount"))
  }

}
