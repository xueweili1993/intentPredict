package test

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xinmei on 16/7/19.
  */
object LDA {

  def main (args: Array[String])= {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)



    val hadoopConf = sc.hadoopConfiguration


    val awsAccessKeyId = args(0)
    val awsSecretAccessKey = args(1)


    //modified by Gao Yuan. 2016-07-11. The ad log data has been backed up in hdfs first.
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
        newtext.toLowerCase.map(_ ->

          (id,_)
        )
      }


      adlog.saveAsTextFile(savepath)





  }


}
