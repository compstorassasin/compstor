import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.9";
  lazy val sparkCore = "org.apache.spark" %% "spark-core" % "3.1.2";
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % "3.1.2";
  lazy val log4jApi = "org.apache.logging.log4j" % "log4j-api" % "2.13.3";
  lazy val log4jCore = "org.apache.logging.log4j" % "log4j-core" % "2.13.3";
}
