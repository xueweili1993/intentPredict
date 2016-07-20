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
    caltoday.add(Calendar.DATE, -2)
    val date = new SimpleDateFormat("yyyyMMdd").format(caltoday.getTime())

    //    val path = "s3n://xinmei-ad-log/ad/ad.log.skip*."+date

    //modified by Gao Yuan. 2016-07-11. The ad log data has been backed up in hdfs first.
    //val path = "hdfs:///gaoy/searchWord/part-00000"
    val path = "s3n://emojikeyboardlite/event/"+date+"/*"

    val savepath = "hdfs:///lxw/awsdata"

    HDFS.removeFile(savepath)
    println("gyy-log path " + path)

    val adlog = sc.textFile(path)
      .flatMap{x =>
        if (x.contains("key_words")){
          Some(x)
        }
        else if (x.contains("hot_words" )){
          Some(x)
        }
        else{
          None
        }
      }

      .saveAsTextFile(savepath)



  }


  }
