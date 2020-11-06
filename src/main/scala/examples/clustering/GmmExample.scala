package examples.clustering

import common.MySparkSession
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.ml.clustering.GaussianMixture

object GmmExample {
  def main(args: Array[String]): Unit = {
    val (spark, sc) = MySparkSession.getDefault(s"${this.getClass.getSimpleName}")
    //    val graph = GraphLoader.edgeListFile(sc, "data/graphx/followers.txt")
    // Loads data
    val dataset = spark.read.format("libsvm").load("data/mllib/sample_kmeans_data.txt")

    dataset.show(20, false)
    dataset.printSchema()
    //+-----+-------------------------+
    //|label|features                 |
    //+-----+-------------------------+
    //|0.0  |(3,[],[])                |
    //|1.0  |(3,[0,1,2],[0.1,0.1,0.1])|
    //|2.0  |(3,[0,1,2],[0.2,0.2,0.2])|
    //|3.0  |(3,[0,1,2],[9.0,9.0,9.0])|
    //|4.0  |(3,[0,1,2],[9.1,9.1,9.1])|
    //|5.0  |(3,[0,1,2],[9.2,9.2,9.2])|
    //+-----+-------------------------+
    //
    //root
    // |-- label: double (nullable = true)
    // |-- features: vector (nullable = true)

    // Trains Gaussian Mixture Model
    val gmm = new GaussianMixture()
      .setK(2)
    val model = gmm.fit(dataset)

    // output parameters of mixture model model
    for (i <- 0 until model.getK) {
      println("weight=%f\nmu=%s\nsigma=\n%s\n" format
        (model.weights(i), model.gaussians(i).mean, model.gaussians(i).cov))
    }
  }
}
