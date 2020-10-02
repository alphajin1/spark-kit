package common

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object MySparkSession {

  def getDefault(appName: String): (SparkSession, SparkContext) = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder.master("local").appName(appName).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    (spark, sc)
  }
}
