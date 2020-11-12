import common.MySparkSession
import examples.graphx.SimRank
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

object SimRankEx01 {

  def main(args: Array[String]): Unit = {
    val logger = Logger.getRootLogger
    val (spark, sc) = MySparkSession.getDefault(s"${this.getClass.getSimpleName}")



    val df = spark.read.csv("data/simrank/simrank_fig1.csv")
    val rawEdges: RDD[(String, String, Double)] = df.rdd.map(x => (x.getString(0), x.getString(1), x.getString(2).toDouble))

    // SimRank [1] 논문의 경우, DirectedGraph 이다.
    val graph = SimRank.getDirectedGraphFromRawEdges(rawEdges)
    val normalizedEdges = SimRank.getNormalizedEdges(graph)
    val resultMatrix = SimRank.getResultMatrix(graph, normalizedEdges)

    logger.warn("[START] resultMatrix")
    SimRank.displayResultMatrix(resultMatrix, graph)
  }
}