package examples.graphx.simrank

import common.MySparkSession
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

object SimRankEx02 {

  def main(args: Array[String]): Unit = {
    val logger = Logger.getRootLogger
    val (spark, sc) = MySparkSession.getDefault(s"${this.getClass.getSimpleName}")

    val df = spark.read.csv("data/simrank/simrank_pp_fig3.csv")
    val rawEdges: RDD[(String, String, Double)] = df.rdd.map(x => (x.getString(0), x.getString(1), x.getString(2).toDouble))

    // SimRank [2] 논문의 경우 *UnDirectedGraph* 이다.
    val graph = SimRank.getUnDirectedGraphFromRawEdges(rawEdges)
    val normalizedEdges = SimRank.getNormalizedEdges(graph)
    val resultMatrix = SimRank.getResultMatrix(graph, normalizedEdges)


    logger.warn("[START] resultMatrix")
    SimRank.displayResultMatrix(resultMatrix, graph)
  }
}
