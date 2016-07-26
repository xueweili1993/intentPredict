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
    val hdfspath = "hdfs:///lxw/fuzzymatch/20160623/*"
    val stoppath = "hdfs:///lxw/stopwords"
    HDFS.removeFile(savepath)

    val stopwords = sc.textFile(stoppath)
      .collect()
      .toSet

    val bStop = sc.broadcast(stopwords)

    val IdWithWord = sc.textFile(hdfspath)
      .flatMap{case line=>

          val linearray = line.split("\t")
          val text ={
            if(linearray.length>2)
              linearray(2)
            else
              ""
          }

          val stop  = bStop.value
          val words = text.split(" ")

          words.map{x=>

            var stemmer = new Stemmer()
            stemmer.add(x.trim())
            if ( stemmer.b.length > 2 )
            {
              stemmer.step1()
              stemmer.step2()
              stemmer.step3()
              stemmer.step4()
              stemmer.step5a()
              stemmer.step5b()
            }
            val x1 = stemmer.b

            if (stop.contains(x1))
              {
                ("","")
              }
            else{
              (linearray(0),x1)
            }
          }
      }
      .filter(x=>

        x._2.length>3
      )
        .cache()


    val wordTable  = IdWithWord.map(x=>
    x._2)
      .distinct()
      .collect()
      .zipWithIndex
      .toMap
     // .foreach()


  }


}
