package test

import java.util.Comparator

import breeze.linalg.min

import com.rockymadden.stringmetric.similarity.DiceSorensenMetric
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import spire.std.boolean

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
    val stopwords  = "hdfs:///lxw/stopwords"

    val savepath = "hdfs:///lxw/test1"
    HDFS.removeFile(savepath)

    val title = findtitle(sc)
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
        (id, newtext)
      }


      .flatMap{case (id, text)=>

        val textarray = text.split(",")
        val length  = textarray.length-1
        var start = length
        while (start-1>=0 && !textarray(start).contains(textarray(start-1)))
          {
            start = start-1
          }
        val kk =  new StringBuilder
        for (i <- start to length)
          {
            kk.append(textarray(i))
            kk.append(" ")
          }

        val newtext= kk.toString.replaceAll(" +"," ")
          newtext.split(" ")
            .map{x=>

              (id,x)
            }

    }

      .mapPartitions{rows=>

        val stopWords = broadstop.value

       val Titles = broadtitle.value

        rows.map{ case (id,word)=>


          val newstring = new StringBuilder



          for (tit <- Titles) {
            val compared = DiceSorensenMetric(1).compare(word, tit)

            val hh = compared match {
              case None => 0

              case Some(compared) => compared
            }

            if (!stopWords.contains(word) && word != "" && hh > 0.5) {
              newstring.append(word)
              newstring.append(",")
              newstring.append(tit)

            }
          }


          (id,newstring.toString())

        }

      }
      /*.filter{case(id,text)=>

          text!=""
      }
      .collect()
      .foreach(x=>

        println ("lxw log "+ x._2)
      )*/
      .saveAsTextFile(savepath)
        /*.mapPartitions{rows=>

          val Titles = broadtitle.value
          var b:Boolean = true

          rows.map {case (id,text)=>

              val wordarray = text.split(" ")

          }*/


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
