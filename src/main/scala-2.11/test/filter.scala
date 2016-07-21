package test

import java.util.Comparator

import breeze.linalg.min
import com.rockymadden.stringmetric.similarity.{DiceSorensenMetric, JaroMetric}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis
import spire.std.boolean

import scala.collection.mutable.ArrayBuffer
import scala.math._

/**
  * Created by xinmei on 16/6/30.
  */
object filter {


  val REDISTTL = 24*3600 //1 day
  val STRATEGY = "FM"

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

    val hdfspath = "hdfs:///lxw/awsdata/*"
    val stopwords  = "hdfs:///lxw/stopwords"

    val savepath = "hdfs:///lxw/test1"
    HDFS.removeFile(savepath)





    val title = findtitle(sc)

      .map{case (title, id, payout) =>

        val newtitle  =  title.replaceAll("[^a-z]"," ").replaceAll(" +"," ").trim
        (newtitle, id, payout)
      }
        .map{case (newtitle, id, payout)=>
            val linearray = newtitle.split(" ")
            if (linearray.length>3)
              {
                var ll = ""
                for (i<-0 to 2)
                  {
                    ll = ll+ " "+linearray(i)
                  }
                (ll.trim,(id,payout))
              }
            else {
              (newtitle.trim,(id,payout))
            }
        }
        .filter{case (newtitle,(id,payout)) =>

         // val linearray = newtitle.split(" ")
          newtitle.length<31 && newtitle.length>4
        }
        .groupByKey
        .map{case (title, adlist)=>

            val newsequence = adlist.toArray.sortWith(_._2>_._2)(0)
          (title, newsequence._1)//(title adid)
        }

      //.saveAsTextFile(savepath)
      .collect
      .toSet

    val broadtitle = sc.broadcast(title)
   // val litedata = getdata.AwsData2process(sc)



   // val mydata = litedata
      val mydata = sc.textFile(hdfspath)
       .flatMap {case line =>

           val kk = line. replaceAll ("\\(|\\)","")
           val linearray = kk.split(",")
         if (linearray.length>1) {
           Some((linearray(0), linearray(1)))
         }else{
           None
         }
         //kk
       }

      .reduceByKey(_+","+_)
      .map { case (id, text)=>

         val newtext = text.replaceAll("\\pP|\\pS"," ").replaceAll(" +"," ")
        (id, newtext.toLowerCase)
      }


      .map{case (id, textwords)=>

          val titles = broadtitle.value
          val adidlist  = new ArrayBuffer[String]()

          title.map{case (pattern,adid)=>


            val sign = StringCompare.fuzzymatch(textwords,pattern,1)


            if (sign){
              adidlist += adid
            }


          }

           (id,textwords,adidlist.toArray.mkString(","))
        }
        .filter{case (id,textwords,adidlist)=>
        adidlist!=""
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

    val sqlcmd = "select title, id, payout from ad where is_deleted = 0"
    //val sqlcmd = "select app_id from app"
    val jdbc = jdbcDF.sqlContext.sql(sqlcmd)
      .map{x =>
        val title  = x(0).toString.toLowerCase()
        val id = x(1).toString
        val payout = x(2).toString.toDouble


        (title, id, payout)
      }

    jdbc

  }


  def save2redis(user2adlist:Array[(String,Array[String])])={

    val jedis = new Jedis("xinmei-ad-ec-redis0.ujh2od.0001.usw2.cache.amazonaws.com")
    val p = jedis.pipelined()
    for(item<- user2adlist){
      val adidlist = STRATEGY + "::" +  item._2.mkString(",")

      p.setex(item._1, REDISTTL,adidlist)


      println("lxw-log id " + item._1)
      println("gyy-log adid " + adidlist)

    }
    p.sync();//这段代码获取所有的response

    p.close()
    jedis.close()

  }




}
