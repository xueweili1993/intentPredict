package test

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xinmei on 16/7/13.
  */
object getdata {


  def main (args: Array[String])= {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)



    val hadoopConf = sc.hadoopConfiguration


    val awsAccessKeyId = args(0)
    val awsSecretAccessKey = args(1)

    val anchordate = "20160426"


    val caltoday = Calendar.getInstance()
    caltoday.add(Calendar.HOUR, -1)
    val date = new SimpleDateFormat("yyyyMMddHH").format(caltoday.getTime())

    //    val path = "s3n://xinmei-ad-log/ad/ad.log.skip*."+date

    //modified by Gao Yuan. 2016-07-11. The ad log data has been backed up in hdfs first.
    val path = "hdfs:///adlog/ad.log.skip*."+date
    val savepath = "hdfs:///lxw/test"

    HDFS.removeFile(savepath)
    println("gyy-log path " + path)

    val adlog = sc.textFile(path)
      .filter{case line=>

          line.contains("3.0.2.0")
      }
      .saveAsTextFile(savepath)



  }


  }
