package examples.graphx

// $example on$

import common.MySparkSession
import org.apache.spark.graphx.GraphLoader
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * A PageRank example on social network dataset
 * Run with
 * {{{
 * bin/run-example graphx.PageRankExample
 * }}}
 */
object PageRankExample {
  def main(args: Array[String]): Unit = {
    val (spark, sc) = MySparkSession.getDefault(s"${this.getClass.getSimpleName}")

    // Load the edges as a graph
    val graph = GraphLoader.edgeListFile(sc, "data/graphx/followers.txt")
    // Run PageRank
    // tol: the tolerance allowed at convergence (smaller => more accurate). (허용오차)
    val ranks = graph.pageRank(0.0001).vertices
    // Join the ranks with the usernames
    val users = sc.textFile("data/graphx/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // Print the result
    println(ranksByUsername.collect().mkString("\n"))
    spark.stop()
  }
}