package test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xinmei on 16/7/22.
  */
object newData {


  def main (args: Array[String])= {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)


    val hadoopConf = sc.hadoopConfiguration


    val awsAccessKeyId = args(0)
    val awsSecretAccessKey = args(1)

    val anchordate = "20160426"

    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    hadoopConf.set("fs.s3n.awsAccessKeyId", awsAccessKeyId)

    hadoopConf.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey)

    val path = "s3://emojikeyboardlite/word/20160720/language=en_*/*"
    //val path  = "hdfs:///lxw/word0/*"
    val savepath  = "hdfs:///lxw/test2"

    HDFS.removeFile(savepath)

    val appset = Array("com.android.chrome","com.google.android.youtube","com.android.browser","com.supercell.clashofclans" ,
      "com.sec.android.app.sbrowser","com.google.android.googlequicksearchbox","com.android.vending",
    "com.ea.gp.nbamobile","com.UCMobile.intl","com.google.android.apps.maps","com.ebay.mobile","com.uc.browser.en",
    "com.lenovo.ideafriend","com.sec.android.app.clockpackage","com.ea.game.maddenmobile15_row")
      .toSet

    val data = sc.textFile(path)
      .map {case line =>

        val linearray = line.split("\t")
        val appname  = linearray(0)
        val usertext = linearray (1)
        val duid  = linearray(2)
        (duid,appname,usertext)
      }
      .filter{case (duid,appname,usertext)=>

          appset.contains(appname)
      }
      .map(x=> (x._1,x._3))
      .reduceByKey(_ + " " + _)
      .map { case (id, text) =>

        val newtext = text.replaceAll("\\pP|\\pS", " ").replaceAll(" +", " ")
        (id, newtext.toLowerCase)
      }
      .saveAsTextFile(savepath)



  }

}
