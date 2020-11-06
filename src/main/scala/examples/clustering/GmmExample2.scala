package examples.clustering

import common.MySparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.clustering.GaussianMixture

import scala.collection.mutable.ListBuffer

object GmmExample2 {

  def main(args: Array[String]): Unit = {

    def gmmUDF(arr :Seq[Seq[Double]]) = {
      var dp = new ListBuffer[Int]
      for (i <- arr.indices) {
        val a = arr(i).head.toInt
        val b = arr(i)(1).round
        for ( j <- 0 until b.toInt) {
          dp.append(a)
        }
      }

//      import spark.implicits._
//      val dpDF = dp.toDF()
//      for (k <- 4 until 7) {
//        print(k)
//        val gmm = new GaussianMixture().setK(k)
//        val model = gmm.fit(dpDF)
//      }

      arr(0)(0)
    }

    val (spark, sc) = MySparkSession.getDefault(s"${this.getClass.getSimpleName}")
    //    val graph = GraphLoader.edgeListFile(sc, "data/graphx/followers.txt")
    // Loads data
    val df = spark.read.json("data/my/targetGMM2.json")

    df.show(20, false)
    df.printSchema()

    df.createOrReplaceTempView("T")
    spark.udf.register("gmmUDF", (seq: Seq[Seq[Double]]) => gmmUDF(seq))
    spark.sql(
      """
        |select gmmUDF(price_c1) res, * from T
        |
        |""".stripMargin
    ).show(200, false)

    // Trains Gaussian Mixture Model
//    val gmm = new GaussianMixture()
//      .setK(2)
//    val model = gmm.fit(df)
//
//    // output parameters of mixture model model
//    for (i <- 0 until model.getK) {
//      println("weight=%f\nmu=%s\nsigma=\n%s\n" format
//        (model.weights(i), model.gaussians(i).mean, model.gaussians(i).cov))
//    }
  }
}
