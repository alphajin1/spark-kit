package common

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object MySparkSession {

  def getDefault(appName: String): (SparkSession, SparkContext) = {
    val spark = SparkSession.builder.master("local").appName(appName).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    (spark, sc)
  }
}
