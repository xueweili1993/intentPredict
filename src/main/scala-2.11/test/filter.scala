package test

import java.util.Comparator

import breeze.linalg.min
import com.rockymadden.stringmetric.similarity.{DiceSorensenMetric, JaroMetric}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import spire.std.boolean

/**
  * Created by xinmei on 16/6/30.
  */
object filter {

  def main (args: Array[String]) {

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
    val stopwords  = "hdfs:///lxw/stopwords"

    val savepath = "hdfs:///lxw/test1"
    HDFS.removeFile(savepath)

    val title = findtitle(sc)
        .filter{case line =>
        line.length<31
        }
      .distinct()
      .collect
      .toSet

    val broadtitle = sc.broadcast(title)

    val Stop = sc.textFile(stopwords)
      .map {case line =>

        line.trim()
      }
        .collect
    .toSet
    val broadstop = sc.broadcast(Stop)

    val test = "candy crash soda saga"

    val mydata = sc.textFile(hdfspath)

      .flatMap {case line =>

          val linearray = line.replaceAll("\\(|\\)","").split(",",4)
        if (linearray.length>3 && linearray(3)!="")
          Some((linearray(0),linearray(1)),linearray(3))
        else
          None
      }
      .reduceByKey(_+","+_)
      .map { case (id, text)=>

         val newtext = text.replaceAll("\\pP|\\pS"," ").replaceAll(" +"," ")
        (id, newtext.toLowerCase)
      }

      .flatMap{case (id, textwords)=>

          val titles = broadtitle.value

          title.map{case pattern=>


            val sign = StringCompare.fuzzymatch(textwords,pattern,5)

            if (sign){
              pattern+ ":"+ textwords
            }
            else{
              ""
            }

          }
        }
    .saveAsTextFile(savepath)



  }



  def findtitle(sc:SparkContext)={


    //val savepath = "hdfs:///lxw/AppwithCate1"

    val sqlContext = new SQLContext(sc)
    val jdbcDF = sqlContext.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://172.31.27.7:3306/koala?user=aduser3&password=VbhaYja_eErJ",
        "dbtable" -> "ad",
        "driver" -> "com.mysql.jdbc.Driver"
      )
    ).load()

    jdbcDF.registerTempTable("ad")

    val sqlcmd = "select title from ad"
    //val sqlcmd = "select app_id from app"
    val jdbc = jdbcDF.sqlContext.sql(sqlcmd)
      .map{x =>
        x(0).toString.toLowerCase()
      }

    jdbc

  }


}
