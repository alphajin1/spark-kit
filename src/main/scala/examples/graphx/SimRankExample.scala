/**
 * Created by gaoshuming on 29/12/2016
 **/


import common.MySparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD

object SimRankExample {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val logger = Logger.getRootLogger
    val (spark, sc) = MySparkSession.getDefault(s"${this.getClass.getSimpleName}")

    val vertices: RDD[(VertexId, (String, String))] =
      sc.parallelize(Seq(
        (1L, ("q", "Univ")),
        (2L, ("q", "ProfA")),
        (3L, ("d", "ProfB")),
        (4L, ("d", "StudentA")),
        (5L, ("d", "StudentB"))
      ))

    val edges: RDD[Edge[Double]] =
      sc.parallelize(Seq(
        Edge(1L, 2L, 1.0),
        Edge(1L, 3L, 1.0),
        Edge(2L, 4L, 1.0),
        Edge(4L, 1L, 1.0),
        Edge(3L, 5L, 1.0),
        Edge(5L, 3L, 1.0)
      ))

    val graph = Graph(vertices, edges)

    // graph.outDegrees : VertexId, Count(OutDegrees)
    val indegrees = graph.inDegrees.map(x => (x._1, x._2.toDouble))
    val normalizedEdges = graph.edges.map {
      x =>
        // Reverse edge
        (x.dstId, (x.srcId, x.attr))
    }.join(indegrees).map {
      x =>
        (
          x._2._1._1, // srcId
          x._1, // dstId
          x._2._1._2 / x._2._2 // Normalize Edge Weight
        )
    }

    val weightMatrix = new CoordinateMatrix(normalizedEdges.map {
      x =>
        MatrixEntry(x._1, x._2, x._3)
    })

    val identityMatrix = new CoordinateMatrix(graph.vertices.map { x =>
      MatrixEntry(x._1, x._1, 1.0)
    })

    val inportanceFactor = 0.8
    var tempMatrix = identityMatrix
    var resultMatrix = tempMatrix
    for (i <- 0 to 10) {
      resultMatrix = new CoordinateMatrix(
        weightMatrix.toBlockMatrix.transpose
          .multiply(tempMatrix.toBlockMatrix)
          .multiply(weightMatrix.toBlockMatrix)
          .toCoordinateMatrix.entries.map {
          x =>

            var w = x.value * inportanceFactor
            if (x.i == x.j && w < 1.0) {
              w = 1.0
            }

            MatrixEntry(x.i, x.j, w)
        }).toBlockMatrix().toCoordinateMatrix()
      tempMatrix = resultMatrix
    }

    val result = resultMatrix.entries.map {
      case MatrixEntry(x, y, w) => (x, y, "%4.3f" format w)
    }
      .map(x => (x._1, (x._2, x._3))) // x, (y, w)
      .join(vertices.map(x => (x._1, x._2._2))) // vertexIdx, xName
      .map(x => (x._2._1._1, (x._2._2, x._2._1._2))) // y, (Name, w)
      .join(vertices.map(x => (x._1, x._2._2))) // vertexIdy, yName
      .map(x => (x._2._1._1, x._2._2, x._2._1._2)) // xName, yName, w
      .filter(x => x._1 != x._2)
      .sortBy(x => (x._1, x._3))

    logger.warn("[START] resultMatrix")
    result.foreach(println)
    //https://github.com/wanesta/Simrank
  }
}