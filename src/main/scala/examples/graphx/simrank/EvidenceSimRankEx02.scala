package examples.graphx.simrank

import common.MySparkSession
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD


object EvidenceSimRankEx02 {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getRootLogger
    val (spark, sc) = MySparkSession.getDefault(s"${this.getClass.getSimpleName}")

    val df = spark.read.csv("data/simrank/simrank_pp_fig4.csv")
    val rawEdges: RDD[(String, String, Double)] = df.rdd.map(x => (x.getString(0), x.getString(1), x.getString(2).toDouble))

    // SimRank [2] 논문의 경우 *UnDirectedGraph* 이다.
    val graph = EvidenceSimRank2.getUnDirectedGraphFromRawEdges(rawEdges)
    val normalizedEdges = EvidenceSimRank2.getWeightedNormalizeEdges(graph)
    val resultMatrix = EvidenceSimRank2.getResultMatrix(graph, normalizedEdges)

    logger.warn("[START] resultMatrix")
    EvidenceSimRank2.displayResultMatrix(resultMatrix, graph)
  }
}
