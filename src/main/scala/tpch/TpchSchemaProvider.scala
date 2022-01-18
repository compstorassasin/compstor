package tpch

import org.apache.spark.sql.types._

object TpchSchemaProvider {
  val lineitem = StructType(
    StructField("l_orderkey", IntegerType, false) ::
      StructField("l_partkey", IntegerType, false) ::
      StructField("l_suppkey", IntegerType, false) ::
      StructField("l_linenumber", IntegerType, false) ::
      StructField("l_quantity", DataTypes.createDecimalType(12, 2), false) ::
      StructField("l_extendedprice", DataTypes.createDecimalType(12, 2), false) ::
      StructField("l_discount", DataTypes.createDecimalType(12, 2), false) ::
      StructField("l_tax", DataTypes.createDecimalType(12, 2), false) ::
      StructField("l_returnflag", StringType, false) ::
      StructField("l_linestatus", StringType, false) ::
      StructField("l_shipdate", DateType, false) ::
      StructField("l_commitdate", DateType, false) ::
      StructField("l_receiptdate", DateType, false) ::
      StructField("l_shipinstruct", StringType, false) ::
      StructField("l_shipmode", StringType, false) ::
      StructField("l_comment", StringType, false) :: Nil)
  val nation = StructType(
    StructField("n_nationkey", IntegerType, false) ::
      StructField("n_name", StringType, false) ::
      StructField("n_regionkey", IntegerType, false) ::
      StructField("n_comment", StringType, false) :: Nil)
  val order = StructType(
    StructField("o_orderkey", IntegerType, false) ::
      StructField("o_custkey", IntegerType, false) ::
      StructField("o_orderstatus", StringType, false) ::
      StructField("o_totalprice", DataTypes.createDecimalType(12, 2), false) ::
      StructField("o_orderdate", DateType, false) ::
      StructField("o_orderpriority", StringType, false) ::
      StructField("o_clerk", StringType, false) ::
      StructField("o_shippriority", IntegerType, false) ::
      StructField("o_comment", StringType, false) :: Nil)
  val region = StructType(
    StructField("r_regionkey", IntegerType, false) ::
      StructField("r_name", StringType, false) ::
      StructField("r_comment", StringType, false) :: Nil)
  val part = StructType(
    StructField("p_partkey", IntegerType, false) ::
      StructField("p_name", StringType, false) ::
      StructField("p_mfgr", StringType, false) ::
      StructField("p_brand", StringType, false) ::
      StructField("p_type", StringType, false) ::
      StructField("p_size", IntegerType, false) ::
      StructField("p_container", StringType, false) ::
      StructField("p_retailprice", DataTypes.createDecimalType(12, 2), false) ::
      StructField("p_comment", StringType, false) :: Nil)
  val customer = StructType(
    StructField("c_custkey", IntegerType, false) ::
      StructField("c_name", StringType, false) ::
      StructField("c_address", StringType, false) ::
      StructField("c_nationkey", IntegerType, false) ::
      StructField("c_phone", StringType, false) ::
      StructField("c_acctbal", DataTypes.createDecimalType(12, 2), false) ::
      StructField("c_mktsegment", StringType, false) ::
      StructField("c_comment", StringType, false) :: Nil)
  val supplier = StructType(
    StructField("s_suppkey", IntegerType, false) ::
      StructField("s_name", StringType, false) ::
      StructField("s_address", StringType, false) ::
      StructField("s_nationkey", IntegerType, false) ::
      StructField("s_phone", StringType, false) ::
      StructField("s_acctbal", DataTypes.createDecimalType(12, 2), false) ::
      StructField("s_comment", StringType, false) :: Nil)
  val partsupp = StructType(
    StructField("ps_partkey", IntegerType, false) ::
      StructField("ps_suppkey", IntegerType, false) ::
      StructField("ps_availqty", IntegerType, false) ::
      StructField("ps_supplycost", DataTypes.createDecimalType(12, 2), false) ::
      StructField("ps_comment", StringType, false) :: Nil)
  def getSchema(tableName: String): StructType =
    tableName match {
      case "lineitem" => lineitem
      case "nation" => nation
      case "order" => order
      case "orders" => order
      case "region" => region
      case "part" => part
      case "customer" => customer
      case "supplier" => supplier
      case "partsupp" => partsupp
    }
}
