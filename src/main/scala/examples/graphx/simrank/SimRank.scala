package examples.graphx.simrank

import org.apache.log4j.Logger
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD

/**
 * SimRank Algorithm
 * Time Complexity = O(Kd^2n^)
 * - K = iteration
 * - d = average degree of a graph
 * - n = number of nodes in a graph
 */
object SimRank {

  val logger = Logger.getRootLogger
  /**
   * UnDirectedGraph 생성 후 반환
   *
   * @param rawEdges
   * @return
   */
  def getUnDirectedGraphFromRawEdges(rawEdges: RDD[(String, String, Double)]) = {
    val nodeWithIndex = rawEdges.map(_._1).union(rawEdges.map(_._2)).distinct().zipWithIndex()
    val vertices: RDD[(VertexId, String)] = nodeWithIndex.map(x => (x._2, x._1))
    val edges: RDD[Edge[Double]] = rawEdges.map {
      x => // srcIdString, dstIdString, Weight
        (x._1, (x._2, x._3))
    }.join(nodeWithIndex).map {
      x => // dstIdString, srcId, Weight
        (x._2._1._1, (x._2._2, x._2._1._2))
    }.join(nodeWithIndex).map {
      x => // srcId, dstId, Weight
        Edge(x._2._1._1, x._2._2, x._2._1._2)
    }

    // UnDirected Graph
    val graph = Graph(vertices, edges.union(edges.map(x => Edge(x.dstId, x.srcId, x.attr))))
    //    그래프 생성 완료 / from Only Edges
    //    graph.triplets.foreach(
    //      x =>
    //        println(x.srcAttr, x.dstAttr, x.attr)
    //    )
    graph
  }

  /**
   * DirectedGraph 생성 후 반환
   * @param rawEdges
   * @return
   */
  def getDirectedGraphFromRawEdges(rawEdges: RDD[(String, String, Double)]) = {
    val nodeWithIndex = rawEdges.map(_._1).union(rawEdges.map(_._2)).distinct().zipWithIndex()
    val vertices: RDD[(VertexId, String)] = nodeWithIndex.map(x => (x._2, x._1))
    val edges: RDD[Edge[Double]] = rawEdges.map {
      x => // srcIdString, dstIdString, Weight
        (x._1, (x._2, x._3))
    }.join(nodeWithIndex).map {
      x => // dstIdString, srcId, Weight
        (x._2._1._1, (x._2._2, x._2._1._2))
    }.join(nodeWithIndex).map {
      x => // srcId, dstId, Weight
        Edge(x._2._1._1, x._2._2, x._2._1._2)
    }

    // UnDirected Graph
    val graph = Graph(vertices, edges)
    graph
  }

  /**
   * Edge Normalize
   * @param graph
   * @return
   */
  def getNormalizedEdges(graph: Graph[String, Double]) = {
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

    normalizedEdges
  }

  /**
   * SimRank 알고리즘의 Matrix 구현 버
   * @param graph
   * @param normalizedEdges
   * @param importantFactor
   * @param iteration
   * @return
   */
  def getResultMatrix(graph: Graph[String, Double], normalizedEdges: RDD[(VertexId, VertexId, Double)], importantFactor: Double = 0.8, iteration: Int = 10) = {
    val numOfVertices = graph.vertices.count()
    val identityMatrix = new CoordinateMatrix(graph.vertices.map { x =>
      MatrixEntry(x._1, x._1, 1.0)
    }, nRows = numOfVertices, nCols = numOfVertices)

    val weightMatrix = new CoordinateMatrix(normalizedEdges.map {
      x =>
        MatrixEntry(x._1, x._2, x._3)
    }, nRows = numOfVertices, nCols = numOfVertices)

    var tempMatrix = identityMatrix
    var resultMatrix = tempMatrix
    for (i <- 0 to iteration) {
      logger.warn(s"Iteration $i started.")

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
        }, nRows = numOfVertices, nCols = numOfVertices)
      tempMatrix = resultMatrix
    }

    resultMatrix
  }

  def displayResultMatrix(resultMatrix: CoordinateMatrix, graph: Graph[String, Double]) = {
    /**
     * 단순한 Display
     * SrcId != DstId
     * SrdId 별로, Similarity 내림차순
     */
    val result = resultMatrix.entries.map {
      case MatrixEntry(x, y, w) => (x, y, "%4.3f" format w)
    }
      .map(x => (x._1, (x._2, x._3))) // x, (y, w)
      .join(graph.vertices.map(x => (x._1, x._2))) // vertexIdx, xName
      .map(x => (x._2._1._1, (x._2._2, x._2._1._2))) // y, (Name, w)
      .join(graph.vertices.map(x => (x._1, x._2))) // vertexIdy, yName
      .map(x => (x._2._1._1, x._2._2, x._2._1._2)) // xName, yName, w
      .filter(x => x._1 != x._2)
      .sortBy(x => (x._1, x._3))

    result.foreach(println)
  }
}
