package examples.graphx.simrank

import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD

object EvidenceSimRank {
  val simRank = SimRank

  def getUnDirectedGraphFromRawEdges(rawEdges: RDD[(String, String, Double)]) = {
    simRank.getUnDirectedGraphFromRawEdges(rawEdges)
  }

  def getNormalizedEdges(graph: Graph[String, Double]) = {
    simRank.getNormalizedEdges(graph)
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
    val tempMatrix = simRank.getResultMatrix(graph, normalizedEdges, importantFactor, iteration)
    val evidenceMatrix = getEvidenceMatrix(graph)

    val resultMatrix = new CoordinateMatrix(
      evidenceMatrix.entries.map {
        x => ((x.i, x.j), x.value)
      }.join(tempMatrix.entries.map {
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

