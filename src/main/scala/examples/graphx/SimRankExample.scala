/**
 * Created by gaoshuming on 29/12/2016
 * */


import common.MySparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class Matrix() {
  def apply(rowCount: Int, colCount: Int)(f: (Int, Int) => Double) = (
    for (i <- 1 to rowCount) yield
      (for (j <- 1 to colCount) yield f(i, j)).toList
    ) //.toList
}

object SimRankExample {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val logger = Logger.getRootLogger()
    val (spark, sc) = MySparkSession.getDefault(s"${this.getClass.getSimpleName}")

    // Create an RDD for the vertices
    val vertices: RDD[(VertexId, (String, String))] =
      sc.parallelize(Seq(
        (1L, ("q", "Univ")),
        (2L, ("q", "ProfA")),
        (3L, ("d", "ProfB")),
        (4L, ("d", "StudentA")),
        (5L, ("d", "StudentB"))
      ))

    // Create an RDD for edges
    val edges: RDD[Edge[Double]] =
      sc.parallelize(Seq(
        Edge(1L, 2L, 1.0),
        Edge(1L, 3L, 1.0),
        Edge(2L, 4L, 1.0),
        Edge(4L, 1L, 1.0),
        Edge(3L, 5L, 1.0),
        Edge(5L, 3L, 1.0)
      ))

    val aEdges = edges.union(edges.map(x => Edge(x.dstId, x.srcId, x.attr)))
    val graph = Graph(vertices, aEdges)

    // graph.outDegrees : VertexId, Count(OutDegrees)
    val outDegrees = graph.outDegrees.map(x => (x._1, (1.toDouble / x._2.toDouble)))
    val normalizedEdges = graph.outerJoinVertices(outDegrees) {
      // vertexId = JoinKey, keyAttr = id or null, joinAttr = weight
      (vertexId, keyAttr, joinAttr) => (vertexId, joinAttr.getOrElse(0))
    }.triplets.map {
      x =>
      // x = triplets
      // SourceId, DestId에 대해 x.srcAttr = Weight, x.attr = 1, 즉 (srcId, dstId, weight)
      val set = (x.srcId, x.dstId, x.srcAttr._2.toString.toDouble * x.attr)
      set
    }

    val weightMatrix = new CoordinateMatrix(normalizedEdges.map {
      x =>
        MatrixEntry(x._1, x._2, x._3)
    })

    val identityMatrix = new CoordinateMatrix(graph.vertices.map { x =>
      MatrixEntry(x._1, x._1, 1.0)
    })

    // 이게... 메트릭스가 돼??
    val delta = 0.8
    var tempMatrix = identityMatrix
    var resultMatrix = tempMatrix
    for (i <- 0 to 10) {
      resultMatrix = new CoordinateMatrix(
        weightMatrix.toBlockMatrix.transpose
          .multiply(tempMatrix.toBlockMatrix)
          .multiply(weightMatrix.toBlockMatrix)
          .toCoordinateMatrix.entries.map {
          x =>

            var w = x.value * delta
            if (x.i == x.j && w < 1.0) {
             w = 1.0
            }

            MatrixEntry(x.i, x.j, w)
        }).toBlockMatrix().toCoordinateMatrix()
      tempMatrix = resultMatrix
      logger.warn(s"[DEBUG] {$i}")
      tempMatrix.entries.map {
        case MatrixEntry(x, y, w) => (x, y, "%4.3f" format w)
      }
        .map(x => (x._1, (x._2, x._3))) // x, (y, w)
        .join(vertices.map(x => (x._1, x._2._2))) // vertexIdx, xName
        .map(x => (x._2._1._1, (x._2._2, x._2._1._2))) // y, (Name, w)
        .join(vertices.map(x => (x._1, x._2._2))) // vertexIdy, yName
        .map(x => (x._2._1._1, x._2._2, x._2._1._2)) // xName, yName, w
        .filter(x => x._1 != x._2)
        .sortBy(x => (x._1, x._3)).foreach(println)
    }

    // 13456
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