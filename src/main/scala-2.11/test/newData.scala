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

    val path = "hdfs:///lxw/test1"
    //val path  = "hdfs:///lxw/word0/*"
    val savepath  = "hdfs:///lxw/test3"

    HDFS.removeFile(savepath)



    val data = sc.textFile(path)
        .map {case line =>


            val linearray = line.split("\\),\\(")
            val k = linearray.length+1
          (k,1)
        }
      .reduceByKey(_+_)
      .saveAsTextFile(savepath)



  }

}
