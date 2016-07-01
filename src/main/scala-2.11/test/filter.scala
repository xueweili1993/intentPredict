package test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xinmei on 16/6/30.
  */
object filter {

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

    val hdfspath = "hdfs:///gaoy/searchWord/part-00000"

    val savepath = "hdfs:///lxw/test"
    HDFS.removeFile(savepath)

    val mydata = sc.textFile(hdfspath)
      .map {case line =>

          val linearray = line.replaceAll("\\(|\\)","").split(",")
        ((linearray(0),linearray(1)),linearray(2))
      }
      .reduceByKey(_+" "+_)
      .saveAsTextFile(savepath)

  }

  }
