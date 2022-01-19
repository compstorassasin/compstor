package tpch

import org.apache.spark.sql.SparkSession
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import org.apache.spark.sql._
import scala.collection.mutable.ListBuffer

/**
  * Parent class for TPC-H queries.
  *
  * Savvas Savvides <savvas@purdue.edu> Orignally wrote
  * ASSASIN authors refactored
  *
  */
abstract class TpchQuery {
  /**
    *  implemented in children classes and hold the actual query
    */
  def execute(
      ss: SparkSession,
      tpchTableProvider: TpchTableProvider,
      outputDir: String
  ): DataFrame
}

object TpchQuery {

  def outputDF(df: DataFrame, outputDir:String): Unit = {
    if (outputDir != "") {
      df.explain(true)
      df.write.mode("overwrite").option("header", "true").csv(outputDir)
    }
  }

  def printUsage(): Unit = {
      println(f"Usage of ${this.getClass.getName}: <inputDir> <outputDir> <queryNum>")
      println(f"    <queryNum> should be 0 to do csv->parquet conversion")
      println(f"    <queryNum> should be 1~22 for querying on parquet source")
      println(f"    <queryNum> should be 26~47 for table slice dump before shuffling")
      println(f"    <queryNum> should be 50~72 for querying on riscvstorage tbl source")
      println(f"    <queryNum> should be 76~97 for querying on csv source")

  }

  def executeQuery(
      ss: SparkSession,
      tpchTableProvider: TpchTableProvider,
      queryNum: Int,
      outputDir: String,
      dumpIntermidiate: Boolean
  ): (String, Float) = {
    val pathStrings = this.getClass.getName.split("\\.").dropRight(1)

    val t0 = System.nanoTime()
    val qid = queryNum % 25
    val qidName = f"Q${qid}%02d"
    val pathStrs = pathStrings :+ qidName
    val query = Class
      .forName(pathStrs.mkString("."))
      .newInstance
      .asInstanceOf[TpchQuery]

    val queryName = f"Q${queryNum}%02d"
    val dirPath = outputDir + "/" + queryName
    outputDF(query.execute(ss, tpchTableProvider, if (dumpIntermidiate) dirPath else ""), if (dumpIntermidiate) "" else dirPath)

    val t1 = System.nanoTime()

    val elapsed = (t1 - t0) / 1000000000.0f // second
    (queryName, elapsed)
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      printUsage
      return
    }

    val inputDir = "file://" + new File(args(0)).getAbsolutePath()
    val outputDir = "file://" + new File(args(1)).getAbsolutePath()
    val arg2_int = args(2).toInt

    if (!(arg2_int == 0 || ((arg2_int % 25) > 0 && (arg2_int % 25 < 23)))) {
      printUsage
      return
    }

    val queryNum = arg2_int
    val useParquetTable = arg2_int < 50
    val dumpIntermidiate = arg2_int > 25 && arg2_int < 48
    val useRISCVTable = arg2_int > 50 && arg2_int < 73

    val ss = SparkSession.builder().appName("TPC-H Q" + queryNum.toString).getOrCreate()

    val tpchTableProvider = new TpchTableProvider(ss, inputDir, useParquetTable, useRISCVTable)

    if (queryNum == 0) {
      tpchTableProvider.customer.write.mode("overwrite").parquet(outputDir + "/customer")
      tpchTableProvider.lineitem.write.mode("overwrite").parquet(outputDir + "/lineitem")
      tpchTableProvider.nation.write.mode("overwrite").parquet(outputDir + "/nation")
      tpchTableProvider.region.write.mode("overwrite").parquet(outputDir + "/region")
      tpchTableProvider.order.write.mode("overwrite").parquet(outputDir + "/orders")
      tpchTableProvider.part.write.mode("overwrite").parquet(outputDir + "/part")
      tpchTableProvider.partsupp.write.mode("overwrite").parquet(outputDir + "/partsupp")
      tpchTableProvider.supplier.write.mode("overwrite").parquet(outputDir + "/supplier")
      return;
    }



    val output = new ListBuffer[(String, Float)]
    output += executeQuery(ss, tpchTableProvider, queryNum, outputDir, dumpIntermidiate)

    val outFile = new File(outputDir.split("//").last + "/TIMES.txt");
    val bw = new BufferedWriter(new FileWriter(outFile, true))

    output.foreach {
      case (key, value) => bw.write(f"${key}%s\t${value}%1.8f\n")
    }

    bw.close()
  }
}
