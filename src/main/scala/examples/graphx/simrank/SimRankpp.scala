package examples.graphx.simrank

import examples.graphx.simrank.SimRank.logger
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SimRankpp {
  /**
   * UnDirectedGraph 생성 후 반환
   *
   * @param rawEdges
   * @return
   */
  def getUnDirectedGraphFromRawEdges(rawEdges: RDD[(String, String, Double)]) = {
    val nodeWithIndex = rawEdges.map(x => (x._1, "q")).union(rawEdges.map(x => (x._2, "d"))).distinct().zipWithIndex()
    val vertices: RDD[(VertexId, (String, String))] = nodeWithIndex.map(x => (x._2, (x._1._1, x._1._2))) // VertexId, String, TypeString
    val edges: RDD[Edge[Double]] = rawEdges.map {
      x => // srcIdString, dstIdString, Weight
        (x._1, (x._2, x._3))
    }.join(vertices.map(x => (x._2._1, x._1))).map {
      x => // dstIdString, srcId, Weight
        (x._2._1._1, (x._2._2, x._2._1._2))
    }.join(vertices.map(x => (x._2._1, x._1))).map {
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

  def getWeightedNormalizeEdges(graph: Graph[(String, String), Double]) = {
    val sumEdges = graph.collectEdges(EdgeDirection.In).map {
      x =>
        val vertexId = x._1
        val neighbors = x._2
        val sumOfEdges = neighbors.map(x => x.attr).sum
        val meanOfEdges = sumOfEdges / neighbors.length
        val variance = neighbors.map(x => Math.pow(x.attr - meanOfEdges, 2)).sum / neighbors.length
        val spread = Math.exp(-variance)
        (vertexId, (sumOfEdges, spread))
    }

    val normalizedEdges = graph.edges.map {
      x =>
        // Reverse edge
        (x.dstId, (x.srcId, x.attr))
    }.join(sumEdges).map {
      x =>
        (
          x._2._1._1, // srcId
          x._1, // dstId
          x._2._2._2 * x._2._1._2 / x._2._2._1 // Normalize Edge Weight
        )
    }


    normalizedEdges
  }

  def getEvidenceMatrix(graph: Graph[(String, String), Double]) = {
    def calculateEvidence(n: Int): Double = {
      var res = 0.0
      var div = 2
      for (i <- 0 until n) {
        res += 1.0 / div
        div *= 2
      }

      res
    }

    val numOfVertices = graph.vertices.count()
    val vertexWithNeighbors = graph.collectNeighborIds(EdgeDirection.In) // v, neighbors
    val vertexWith2distVertex = vertexWithNeighbors.flatMap {
      x => x._2.map(y => (y, x._1)) // (neighborVertex, vertex)
    }.join(vertexWithNeighbors).map {
      x => (x._2._1, x._2._2) // vertex, neighborsOfNeighbors
    }.flatMap {
      x => x._2.map(y => (y, x._1)) // vertex, vertex with 2 dist
    }

    val evidenceMatrix = new CoordinateMatrix(
      vertexWith2distVertex.join(vertexWithNeighbors).map {
        x => (x._2._1, (x._1, x._2._2)) // 2dist vertex, vertex, neighbors(v)
      }.join(vertexWithNeighbors).map {
        x =>
          val srcId = x._2._1._1
          val srcNeighbors = x._2._1._2
          val dstId = x._1
          val dstNeighbors = x._2._2

          val commonEdges = srcNeighbors.intersect(dstNeighbors).length
          (srcId, dstId, calculateEvidence(commonEdges))
      }.map {
        e => (e._1, e._2, e._3)
      }.filter(x => x._3 > 0.0 && x._1 != x._2).map {
        x => MatrixEntry(x._1, x._2, x._3)
      }, nRows = numOfVertices, nCols = numOfVertices
    )

    evidenceMatrix
  }

  def getResultMatrix(graph: Graph[(String, String), Double], normalizedEdges: RDD[(VertexId, VertexId, Double)], importantFactor: Double = 0.8, iteration: Int = 10) = {
    val numOfVertices = graph.vertices.count()
    val tempGraph = Graph(graph.vertices, normalizedEdges.map(x => Edge[Double](x._1, x._2, x._3.toDouble)))
    val initVertices = tempGraph.collectEdges(EdgeDirection.In).map {
      x =>
        val vertexId = x._1
        val neighbors = x._2
        val sumOfEdges = neighbors.map(x => x.attr).sum

        (vertexId, sumOfEdges)
    }

    val identityMatrix = new CoordinateMatrix(graph.vertices.map { x =>
      MatrixEntry(x._1, x._1, 1)
    }, nRows = numOfVertices, nCols = numOfVertices)


    val weightMatrix = new CoordinateMatrix(normalizedEdges
      .map {
        x =>
          MatrixEntry(x._1, x._2, x._3)
      }, nRows = numOfVertices, nCols = numOfVertices)

    // CustomMultiply
    def multiply(left: CoordinateMatrix, right: CoordinateMatrix): CoordinateMatrix = {
      val leftEntries = left.entries.map({ case MatrixEntry(i, j, v) => (j, (i, v)) })
      val rightEntries = right.entries.map({ case MatrixEntry(j, k, w) => (j, (k, w)) })

      val productEntries = leftEntries
        .join(rightEntries)
        .map({ case (_, ((i, v), (k, w))) => ((i, k), (v * w)) })
        .reduceByKey(_ + _)
        .map({ case ((i, k), sum) => MatrixEntry(i, k, sum) })

      new CoordinateMatrix(productEntries)
    }

    var tempMatrix = identityMatrix
    var atsaMatrix = tempMatrix
    for (i <- 0 to iteration) {
      logger.warn(s"Iteration $i started.")
      atsaMatrix = new CoordinateMatrix(
        multiply(multiply(weightMatrix.transpose, tempMatrix), weightMatrix)
          .entries.map {
          x =>

            var w = x.value * importantFactor
            if (x.i == x.j && w < 1.0) {
              w = 1.0
            }

            MatrixEntry(x.i, x.j, w)
        }, nRows = numOfVertices, nCols = numOfVertices)

      tempMatrix = atsaMatrix
    }

    val evidenceMatrix = getEvidenceMatrix(graph)
    val resultMatrix = new CoordinateMatrix(
      evidenceMatrix.entries.map {
        x => ((x.i, x.j), x.value)
      }.join(atsaMatrix.entries.map {
        x => ((x.i, x.j), x.value)
      }).map {
        x => MatrixEntry(x._1._1, x._1._2, x._2._1 * x._2._2)
      }.filter {
        x => x.value > 0
      }
    )

    resultMatrix
  }

  def getResultDataFrame(spark: SparkSession, resultMatrix: CoordinateMatrix, graph: Graph[(String, String), Double]) = {
    /**
     * 단순한 Display
     * SrcId != DstId
     * SrdId 별로, Similarity 내림차순
     */

    val queryVertices = graph.vertices.filter(x => x._2._2 == "q")
    val result = resultMatrix.entries.map {
      case MatrixEntry(x, y, w) => (x, y, "%4.3f" format w)
    }
      .map(x => (x._1, (x._2, x._3))) // x, (y, w)
      .join(queryVertices.map(x => (x._1, x._2._1))) // vertexIdx, xName
      .map(x => (x._2._1._1, (x._2._2, x._2._1._2))) // y, (Name, w)
      .join(queryVertices.map(x => (x._1, x._2._1))) // vertexIdy, yName
      .map(x => (x._2._1._1, x._2._2, x._2._1._2)) // xName, yName, w
      .filter(x => x._1 != x._2)
      .sortBy(x => (x._1, x._3))

    spark.createDataFrame(result).toDF("query", "relQueries", "weight")
  }
}
