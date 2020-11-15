package examples.graphx.simrank

import examples.graphx.simrank.EvidenceSimRank.simRank
import examples.graphx.simrank.SimRank.logger
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD

object EvidenceSimRank2 {

  val simRank = SimRank
  def getUnDirectedGraphFromRawEdges(rawEdges: RDD[(String, String, Double)]) = {
    simRank.getUnDirectedGraphFromRawEdges(rawEdges)
  }
  def getWeightedNormalizeEdges(graph: Graph[String, Double]) = {
    val sumEdges = graph.collectEdges(EdgeDirection.In).map {
      x =>
        val vertexId = x._1
        val neighbors = x._2
        val sumOfEdges = neighbors.map(x => x.attr).sum
        (vertexId, sumOfEdges)
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
          x._2._1._2 / x._2._2 // Normalize Edge Weight
        )
    }


    normalizedEdges
  }

  def getEvidenceMatrix(graph: Graph[String, Double]) = {
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
    val neighbors = graph.collectNeighborIds(EdgeDirection.In)
    val evidenceMatrix = new CoordinateMatrix(
      neighbors.cartesian(neighbors).map {
        x =>
          val srcId = x._1._1
          val srcNeighbors = x._1._2
          val dstId = x._2._1
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

  def getResultMatrix(graph: Graph[String, Double], normalizedEdges: RDD[(VertexId, VertexId, Double)], importantFactor: Double = 0.8, iteration: Int = 10) = {
    val numOfVertices = graph.vertices.count()
    val identityMatrix = new CoordinateMatrix(graph.vertices.map { x =>
      MatrixEntry(x._1, x._1, 1.0)
    }, nRows = numOfVertices, nCols = numOfVertices)

    val weightMatrix = new CoordinateMatrix(normalizedEdges
      .map {
      x =>
        MatrixEntry(x._1, x._2, x._3)
    }, nRows = numOfVertices, nCols = numOfVertices)

    var tempMatrix = identityMatrix
    var atsaMatrix = tempMatrix
    for (i <- 0 to iteration) {
      logger.warn(s"Iteration $i started.")
      atsaMatrix = new CoordinateMatrix(
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

  def displayResultMatrix(resultMatrix: CoordinateMatrix, graph: Graph[String, Double]) = {
    simRank.displayResultMatrix(resultMatrix, graph)
  }
}
