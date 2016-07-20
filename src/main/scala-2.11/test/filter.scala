package test

import java.util.Comparator

import breeze.linalg.min
import com.rockymadden.stringmetric.similarity.{DiceSorensenMetric, JaroMetric}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import spire.std.boolean

import scala.collection.mutable.ArrayBuffer
import scala.math._

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

      .map{case line =>

        line.replaceAll("[^a-z1-9]"," ").replaceAll(" +"," ").trim
      }
        .filter{case line =>

          val linearray = line.split(" ")
          line.length<31 && line.length>4 && linearray.length>1
        }

      .distinct()
      //.saveAsTextFile(savepath)
      .collect
      .toSet

    val broadtitle = sc.broadcast(title)

   /* val Stop = sc.textFile(stopwords)
      .map {case line =>

        line.trim()
      }
        .collect
    .toSet
    val broadstop = sc.broadcast(Stop)*/



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


            val sign = StringCompare.fuzzymatch(textwords,pattern,1)

            if (sign){
              pattern+ ":"+ textwords
            }
            else{
              ""
            }

          }
        }
        .filter{case line=>
        line!=""
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


  def fuzzymatch (target: String, pattern: String, k:Int)={

    val pl = pattern.length
    val tl = target.length


    val B = new ArrayBuffer[(Char,Int)]()
    for (i <- 0 to pl-1){
      val pat = pattern.charAt(i)

      val indexarray = getindex(pattern, pat)
      B += ((pat,Integer.valueOf(indexarray.toString(),2)))

    }

    val Bmap = B.toMap



    val R = new Array[Int](k+1)
    for (i<- 0 to k){

      R(i) = pow(2.0,i).toInt-1
    }


    var sign = false
    //var pos = ""


    for (position<- 0 to tl-1) {

      val targetchar = target.charAt(position)

      val getstatus =

        Bmap.get(targetchar) match {
          case Some(x) => x
          case None => 0

        }

      var Rold = R(0)
      var Rnew = ((Rold << 1) | 1) & getstatus
      R(0) = Rnew

      for (i<- 1 to k){

        Rnew = ((R(i) << 1) & getstatus) | Rold | ((Rold | Rnew) << 1) | 1

        Rold = R(i)
        R(i) = Rnew
      }



      if((pow(2,pl-1).toInt & Rnew )!=0){

        println ("yes"+" "+ targetchar)

        sign = true
        //pos = target.substring(max(0,position-pl+1), position+1)

      }
    }
    sign

  }


  def getindex(Str:String, C:Char)={

    val chararray = Str.toCharArray

    val length = Str.length

    val label = new StringBuilder
    for(i<- 0 to length-1)
    {

      if (chararray(length-1-i)==C){
        label.append("1")
      }
      else{
        label.append("0")
      }
    }

    label
  }


}
