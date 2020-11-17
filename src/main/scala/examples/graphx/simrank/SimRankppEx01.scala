package examples.graphx.simrank

import common.MySparkSession
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

object SimRankppEx01 {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getRootLogger
    val (spark, sc) = MySparkSession.getDefault(s"${this.getClass.getSimpleName}")

    val df = spark.read.csv("hidden/relqueries_sample.csv")
    val rawEdges: RDD[(String, String, Double)] = df.rdd.map(x => (x.getString(0), x.getString(1), x.getString(3).toDouble))
//
//    val df = spark.read.csv("data/simrank/simrank_pp_fig3.csv")
//    val rawEdges: RDD[(String, String, Double)] = df.rdd.map(x => (x.getString(0), x.getString(1), x.getString(2).toDouble))

    // SimRank [2] 논문의 경우 *UnDirectedGraph* 이다.
    val graph = SimRankpp.getUnDirectedGraphFromRawEdges(rawEdges)
    val normalizedEdges = SimRankpp.getSpreadNormalizedEdges(graph)
    val resultMatrix = SimRankpp.getResultMatrix(graph, normalizedEdges, importantFactor = 0.8, deltaThreshold = 0.05, iteration = 1)

    logger.warn("[START] resultMatrix")
    val result1DF = SimRankpp.getResultDataFrame(spark, resultMatrix, graph)
    import spark.implicits._
    result1DF.filter($"query" === "스타벅스").orderBy($"weight".desc).show(100, false)
  }
}
