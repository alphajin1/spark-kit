package examples.embedding

import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import common.MySparkSession
import org.apache.log4j.Logger
import org.apache.spark.mllib.linalg.Vectors

object Word2VecEx01 {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getRootLogger
    val (spark, sc) = MySparkSession.getDefault(s"${this.getClass.getSimpleName}")


    val input = sc.textFile("data/mllib/sample_lda_data.txt").map(line => line.split(" ").toSeq)

    val word2vec = new Word2Vec().setVectorSize(100).setMinCount(1).setNumIterations(10).setSeed(42).setWindowSize(2)

    val model = word2vec.fit(input)

    val synonyms = model.findSynonyms("1", 5)
//    val vector1 = Vectors.dense(model.getVectors("1").toSeq)
//
//    vector1 -
//    model.getVectors("2")(0)
//    model.getVectors("3")(0)
    for ((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

    // Save and load model
//    model.save(sc, "myModelPath")
//    val sameModel = Word2VecModel.load(sc, "myModelPath")

//    val w2v_map = model.getVectors//this gives u a map {word:vec}
//
//    val (king, man, woman) = (w2v_map("king"), w2v_map("man"), w2v_map("women"))
//
//    val n = king.length

    //daxpy(n: Int, da: Double, dx: Array[Double], incx: Int, dy: Array[Double], incy: Int);
//    blas.saxpy(n,-1,man,1,king,1)
//
//    blas.saxpy(n,1,woman,1,king,1)
//
//    val vec = new DenseVector(king.map(_.toDouble))

//    val most_similar_word_to_vector = sameModel.findSynonyms(vec, 10) //they have an api to get synonyms for word, and one for vector
//    for((synonym, cosineSimilarity) <- most_similar_word_to_vector) {
//      println(s"$synonym $cosineSimilarity")
//    }
  }
}
