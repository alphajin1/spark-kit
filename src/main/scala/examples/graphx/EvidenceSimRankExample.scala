import common.MySparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD

object EvidenceSimRankExample {

  // TODO 작업중, 아직 EvidenceSimRank가 아님!
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val logger = Logger.getRootLogger
    val (spark, sc) = MySparkSession.getDefault(s"${this.getClass.getSimpleName}")

    val rawEdges: RDD[(String, String, Double)] = sc.parallelize(
      Seq(
        ("pc", "Hp.com", 1),
        ("camera", "Hp.com", 1),
        ("camera", "Bestbuy.com", 1),
        ("Digital camera", "Hp.com", 1),
        ("Digital camera", "Bestbuy.com", 1),
        ("tv", "Bestbuy.com", 1),
        ("flower", "Teleflora.com", 1),
        ("flower", "Orchids.com", 1)
      )
    )

    val nodeWithIndex = rawEdges.map(_._1).union(rawEdges.map(_._2)).distinct().zipWithIndex()

    val vertices: RDD[(VertexId, String)] = nodeWithIndex.map(x => (x._2, x._1))
    val edges: RDD[Edge[Double]] = rawEdges.map {
      x =>
        // srcIdString, dstIdString, Weight
        (x._1, (x._2, x._3))
    }.join(nodeWithIndex).map {
      x =>
        // dstIdString, srcId, Weight
        (x._2._1._1, (x._2._2, x._2._1._2))
    }.join(nodeWithIndex).map {
      x =>
        // srcId, dstId, Weight
        Edge(x._2._1._1, x._2._2, x._2._1._2)
    }

    // UnDirected Graph
    val graph = Graph(vertices, edges.union(edges.map(x => Edge(x.dstId, x.srcId, x.attr))))
    //    그래프 생성 완료 / from Only Edges
    //    graph.triplets.foreach(
    //      x =>
    //        println(x.srcAttr, x.dstAttr, x.attr)
    //    )

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


    val identityMatrix = new CoordinateMatrix(graph.vertices.map { x =>
      MatrixEntry(x._1, x._1, 1.0)
    })

    val weightMatrix = new CoordinateMatrix(normalizedEdges.map {
      x =>
        MatrixEntry(x._1, x._2, x._3)
    }, nRows = vertices.count(), nCols = vertices.count())

    val importantFactor = 0.8
    var tempMatrix = identityMatrix
    var resultMatrix = tempMatrix
    for (i <- 0 to 10) {
      resultMatrix = new CoordinateMatrix(
        weightMatrix.toBlockMatrix.transpose
          .multiply(tempMatrix.toBlockMatrix)
          .multiply(weightMatrix.toBlockMatrix)
          .toCoordinateMatrix.entries.map {
          x =>

            var w = x.value * importantFactor
            if (x.i == x.j && w < 1.0) {
              w = 1.0
            }

            MatrixEntry(x.i, x.j, w)
        }) // .toBlockMatrix().toCoordinateMatrix()
      tempMatrix = resultMatrix
    }

    val result = resultMatrix.entries.map {
      case MatrixEntry(x, y, w) => (x, y, "%4.3f" format w)
    }
          .map(x => (x._1, (x._2, x._3))) // x, (y, w)
          .join(vertices.map(x => (x._1, x._2))) // vertexIdx, xName
          .map(x => (x._2._1._1, (x._2._2, x._2._1._2))) // y, (Name, w)
          .join(vertices.map(x => (x._1, x._2))) // vertexIdy, yName
          .map(x => (x._2._1._1, x._2._2, x._2._1._2)) // xName, yName, w
          .filter(x => x._1 != x._2)
          .sortBy(x => (x._1, x._3))

    logger.warn("[START] resultMatrix")
    result.foreach(println)
    //https://github.com/wanesta/Simrank
  }
}