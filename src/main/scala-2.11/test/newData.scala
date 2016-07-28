package test



import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by xinmei on 16/7/22.
  */
object newData {


  def main (args: Array[String])= {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)


    val hadoopConf = sc.hadoopConfiguration



    val path = "hdfs:///lxw/sample_lda_da"
    val savepath  = "hdfs:///lxw/test3"

    HDFS.removeFile(savepath)

    // Load and parse the data


    val data = sc.textFile(path)
    val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble)))
    // Index documents with unique IDs
    val corpus = parsedData.zipWithIndex.map(_.swap).cache()

    // Cluster the documents into three topics using LDA
    val ldaModel = new LDA().setK(3).run(corpus)

    // Output topics. Each is a distribution over words (matching word count vectors)
    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
    val topics = ldaModel.topicsMatrix
    for (topic <- Range(0, 3)) {
      print("Topic " + topic + ":")
      for (word <- Range(0, ldaModel.vocabSize)) { print(" " + topics(word, topic)); }
      println()
    }






  }

}
