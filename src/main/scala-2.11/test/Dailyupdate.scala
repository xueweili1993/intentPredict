package test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xinmei on 16/7/26.
  */
object Dailyupdate {

  def main (args: Array[String])= {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)


    val hadoopConf = sc.hadoopConfiguration


    val path = "hdfs:///lxw/awsdata"

    val savepath  = "hdfs:///lxw/test3"

    HDFS.removeFile(savepath)

    val Recommodation = sc.textFile(path)
      .flatMap{case line=>

        val newstring = line.substring(78, line.length)
        val linearray = line.split("\t")
          linearray.map{x =>

            val pair  = x.replaceAll("\\(|\\)","").split(",")


              val adid = pair(0)
              val title = pair(1)

            (adid, title)

          }

      }




  }


}
