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
  def createGraph(edges: RDD[(String, String)]): Graph[String, String] = {
    // vertices : originId, assignedId
    val vertices = (edges.map(_._1) union (edges.map(_._2))).distinct.zipWithIndex()
    //val relationships: RDD[Edge[String]]  = nods.join(indnode).map(x=>x._2).join(indnode).map(_._2).map { x =>
    val relationships = edges.map { x =>
      val x1 = (x._1 + "L").replace("L", "").toLong
      val x2 = (x._2 + "L").replace("L", "").toLong
      val ed = Edge(x1, x2, "1")
      ed
    }.groupBy {
      x => (x.srcId, x.dstId) // srcId, dstId, attr
    }.map {
      x =>
        // x._1 = (srcId, dstId), x._2 = CompactBuffer(List)
        // set = srcId, dstId, weight(String, Length)
        val set = Edge(x._1._1.toString.toLong, x._1._2.toString.toLong, x._2.toVector.length.toString)
        set
    }

    val users = vertices.map(line => (line, "id")).map { x =>
      val x1 = x._1._2 // x1 = assignedId
      (x1, x._2) // (assignedId, id) pair
    }

    // id가 있으면, MappingId 이고, 아니면 OriginalId
    val graph = Graph(users, relationships)
    graph
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val (spark, sc) = MySparkSession.getDefault(s"${this.getClass.getSimpleName}")
    val delta = 0.8

    val filePath = "data/graphx/simrank_data.txt"
    val file = sc.textFile(filePath)
    val edges: RDD[(String, String)] = file.map { x =>
      val raw_edges = (x.split("\t")(0), x.split("\t")(1))
      raw_edges
    }

    val vertices = (edges.map(_._1) union (edges.map(_._2))).distinct.zipWithIndex()
    var numOfVertices = vertices.collect.length
    // numOfVertices: 327
    // numOfEdges : 6810

    val graph = createGraph(edges)
    // outDegrees = VertexId, Count(OutDegrees)
    // DEBUG : graph.edges.collect().filter(x => x.srcId == 5812) = 19개, 1/19로 변환
    val outDegrees = graph.outDegrees.map(x => (x._1, (1.toDouble / x._2.toDouble)))
    val normalizedEdges = graph.outerJoinVertices(outDegrees) {
      // assignedId 이면 weight = 0
      (id, _, weight) => (id.toString, weight.getOrElse(0))
    }.triplets.map { x =>
      // SourceId, DestId에 대해 x.srcAttr = Weight, x.attr = 1
      val set = (x.srcId, x.dstId, x.srcAttr._2.toString.toDouble * x.attr.toInt)
      set
    }

    // 6810
    normalizedEdges.take(5).foreach(println)

    //val sg = stgra//.map(_._1).union(stgra.map(_._2))
    // 164
    val indnode = (normalizedEdges.map(_._1).union(normalizedEdges.map(_._2))).distinct.zipWithIndex()
    val sgnu = normalizedEdges.map(x => (x._1, (x._2, x._3))).join(indnode).map(x => (x._2._1._1, (x._2._1._2, x._2._2))).join(indnode).map(x => (x._2._1._2, x._2._2, x._2._1._1))
    sgnu.take(5).foreach(println)
    val sg = sgnu.map(x => (x._2, x._1, x._3)).union(sgnu.map(x => (x._1, x._2, 1.toDouble / numOfVertices)))

    numOfVertices = sg.count.toInt
    //    val inputGraph: Graph[PartitionID, String] = graph.outerJoinVertices(graph.outDegrees)((vid, _, degOpt) => degOpt.getOrElse(0))
    //    // Construct a graph where each edge contains the weight
    //    // and each vertex is the initial PageRank
    //    val node_len = graph.triplets.map(trip => trip.srcId).union(graph.triplets.map(trip => trip.dstId)).distinct.count.toInt //获取所有节点总数
    //
    //    val m_list = new Matrix() //生成矩阵格式的List
    //
    //    //val node1 = gra.edges.map(x => x.relativeDirection(EdgeDirection.Either)).collect
    //    //all the input neighbors
    //    val maptab = graph.collectNeighborIds(EdgeDirection.Either).map { x =>
    //      val set = (x._1, x._2.toVector, 1.toDouble / x._2.length.toDouble)
    //      set
    //    }

    // 164 * 164
    // 와 진짜 이거 계산을 어떻게 하란거지?
    val matrixprob = new CoordinateMatrix(sg.map { x => //概率转移矩阵
      val s = MatrixEntry(x._1, x._2, x._3)
      s
    })

    numOfVertices = matrixprob.numRows.toInt
    val matrixp = new CoordinateMatrix(matrixprob.entries.map { x =>
      val set = MatrixEntry(x.i, x.i, 1.0)
      set
    })
    matrixprob.entries.take(5).foreach(println)
    //    val unit = new CoordinateMatrix(matrixp.entries.map { x => (x.i, x.i, x.value) }.union(matrixp.entries.map(x => (x.j, x.j, x.value))).map { x =>
    //      val set = MatrixEntry(x._1, x._2, x._3 * delta)
    //      set
    //    }.distinct)
    val In = new CoordinateMatrix(matrixp.entries.map { x => //s0 = (1-C)In
      val set = MatrixEntry(x.i, x.j, x.value * (1 - delta))
      set
    })

    val S0 = In //初始化S0
    var Sk = S0
    var Skp1 = Sk

    //var mx= Skp1.toBlockMatrix.subtract(Sk.toBlockMatrix()).toCoordinateMatrix.entries.map(_.value.abs).max
    for (i <- 0 to 10) { //mx >= math.pow(delta,k)) {
      //println( "Ck -> "+"%.10f" format math.pow(delta,k))
      Skp1 = new CoordinateMatrix(Sk.toBlockMatrix.multiply(matrixprob.toBlockMatrix.transpose).multiply(matrixprob.toBlockMatrix).toCoordinateMatrix.entries.map { x =>
        val set = MatrixEntry(x.i, x.j, x.value * delta)
        set
      }).toBlockMatrix.add(In.toBlockMatrix).toCoordinateMatrix()
      //mx = Skp1.toBlockMatrix.subtract(Sk.toBlockMatrix()).toCoordinateMatrix.entries.map(_.value.abs).max
      Sk = Skp1
      //println("MAX -> " + "%.10f" format mx)
    }
    val result = Skp1.entries.map {
      case MatrixEntry(x, y, j) => (x, y, "%4.3f" format j)
    }.map(x => (x._1, (x._2, x._3))).join(indnode.map(x => (x._2, x._1.toLong)))
      .map(x => (x._2._1._1, (x._2._1._2, x._2._2))).join(indnode.map(x => (x._2, x._1.toLong)))
      .map(x => (x._2._1._2, x._2._2, x._2._1._1))

    result.filter(_._3.toDouble < 1.0).saveAsTextFile("data/output/graphx/simrank_data")
  }
}