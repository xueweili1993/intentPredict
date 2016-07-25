package test

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{LDA, DistributedLDAModel}


/**
  * Created by xinmei on 16/7/19.
  */
object myLDA {

  def main (args: Array[String]) ={

    val conf = new SparkConf()

    val sc = new SparkContext(conf)



    val hadoopConf = sc.hadoopConfiguration


    val awsAccessKeyId = args(0)
    val awsSecretAccessKey = args(1)



    val savepath = "hdfs:///lxw/test1"
    val hdfspath = "hdfs:///lxw/awsdata/*"
    HDFS.removeFile(savepath)


    val adlog = sc.textFile(hdfspath)

      .flatMap {case line =>

        val linearray = line.replaceAll("\\(|\\)","").split(",",4)
        if (linearray.length>3 && linearray(3)!="")
          Some((linearray(0),linearray(1)),linearray(3))
        else
          None
      }
      .reduceByKey(_+","+_)
      .flatMap { case (id, text)=>

        val newtext = text.replaceAll("\\pP|\\pS"," ").replaceAll(" +"," ")
        newtext.toLowerCase.split(" ")map { x =>

          ((id, x),1)
        }
      }
      .reduceByKey(_+_)
      .map { case ((id, x), num) =>

        (id,(x,num))
      }
      .groupByKey()

      val nums = adlog.map{case (id,array)=>

        val vec = Vectors.dense(array.map(x=>x._2.toDouble).toArray)
        val words = array.map(x=>x._1).toArray
        (words,vec)
      }
      val pp = nums.map(x=> x._2)

    val corpus = pp.zipWithIndex.map(_.swap).cache()

    val ldaModel = new LDA().setK(3).run(corpus)

    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
    //val topics = ldaModel.topicsMatrix
   /* for (topic <- Range(0, 3)) {
      print("Topic " + topic + ":")
      for (word <- Range(0, ldaModel.vocabSize)) { print(" " + topics(word, topic)); }
      println()
    }*/


    //adlog.saveAsTextFile(savepath)

  }


}
