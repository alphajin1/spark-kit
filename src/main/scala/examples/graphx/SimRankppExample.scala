package examples.graphx

import common.MySparkSession
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId}
import org.apache.spark.rdd.RDD


class SimRankppAlgo(vertices: RDD[(VertexId, (String, String))], edges: RDD[Edge[Double]], decay: Double, delta: Double = 0.05) {
  val graph = Graph(vertices, edges)


  def train(): Unit = {

  }

  def sayHello(): Unit = {
    println("***************************")
    println("Hello, World!")
    println("vertices : ", vertices.count())
    println("edges : ", edges.count())
    println("***************************")
  }
}

object SimRankppExample {

  def main(args: Array[String]): Unit = {
    val (spark, sc) = MySparkSession.getDefault(s"${this.getClass.getSimpleName}")

    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Seq(
        (1L, ("q", "Univ")),
        (2L, ("q", "ProfA")),
        (3L, ("d", "ProfB")),
        (4L, ("d", "StudentA")),
        (5L, ("d", "StudentB"))
      ))

    // Create an RDD for edges
    val relationships: RDD[Edge[Double]] =
      sc.parallelize(Seq(
        Edge(1L, 2L, 1.0),
        Edge(1L, 3L, 1.0),
        Edge(2L, 4L, 1.0),
        Edge(4L, 1L, 1.0),
        Edge(3L, 5L, 1.0),
        Edge(5L, 3L, 1.0)
      ))

    val graph = Graph(users, relationships)
    graph.vertices.take(10).foreach(println)
    graph.edges.take(10).foreach(println)

    val model = new SimRankppAlgo(users, relationships, 0.8)
    model.sayHello()

    val userList = users.collect().toList
    for (i <- userList.indices) {
      for (j <- userList.indices) {
        if (i < j) {
          val score = 1.0
          if (score > 0.0) {
            println(userList(i)._2._2, userList(j)._2._2, score)
          }
        }
      }
    }
  }
}
