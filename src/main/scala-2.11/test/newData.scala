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

    val path  = "s3n://emojikeyboardlite/word/20160720/language=en_*/*"
    val savepath  = "hdfs:///lxw/test2"

    HDFS.removeFile(savepath)

    val data = sc.textFile(path)
      .map {case line =>

          val linearray = line.split("\t")
        (linearray(0),1)
      }
      .reduceByKey(_+_)
      .sortBy(_._2)
      .saveAsTextFile(savepath)




  }

}
