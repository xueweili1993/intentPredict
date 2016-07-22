package test

import java.text.SimpleDateFormat
import java.util.Calendar

import com.sanoma.cda.geoip.MaxMindIpGeo
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject


/**
  * Created by xinmei on 16/7/13.
  */
object getdata {


  def main (args: Array[String])= {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val mmdbPath = "/home/gaoyuan/userLabel/resource/GeoIP2-City.mmdb"
    sc.addFile(mmdbPath)



    val hadoopConf = sc.hadoopConfiguration


    val awsAccessKeyId = args(0)
    val awsSecretAccessKey = args(1)

    val anchordate = "20160426"

    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    hadoopConf.set("fs.s3n.awsAccessKeyId", awsAccessKeyId)

    hadoopConf.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey)



    //    val path = "s3n://xinmei-ad-log/ad/ad.log.skip*."+date

    //modified by Gao Yuan. 2016-07-11. The ad log data has been backed up in hdfs first.
    //val path = "hdfs:///gaoy/searchWord/part-00000"
    //val path = "s3n://emojikeyboardlite/event/"+date+"/*"



    val caltoday = Calendar.getInstance()
    caltoday.add(Calendar.DATE, -2)
    val date = new SimpleDateFormat("yyyyMMdd").format(caltoday.getTime())

    val ltvpath = "s3n://emojikeyboardlite/ltv/"+date+"/*"

    val eventpath = "s3n://emojikeyboardlite/event/"+date+"/*"

    val savepath = "hdfs:///lxw/fuzzymatch/"+date

    HDFS.removeFile(savepath)

    //=====duid, countryCode, gaid, aid====
    val Metacountry = AwsMeta(sc,ltvpath)


    //======id, hot_words=======

    val DataWithCountry = AwsData2process (sc:SparkContext,eventpath)
      .reduceByKey(_+","+_)
      .map { case (id, text)=>

        val newtext = text.replaceAll("\\pP|\\pS"," ").replaceAll(" +"," ")
        (id, newtext.toLowerCase)
      }
      .join(Metacountry)
      .map {case (duid, (hot_words,(countryCode,gaid,aid)))=>

          gaid+"_"+aid+"\t"+countryCode+"\t"+hot_words
      }
      .saveAsTextFile(savepath)


  }




  def AwsMeta (sc:SparkContext,path:String)={


    println("gyy-log path " + path)

    val userinfor = sc.textFile(path)
      .flatMap{case line =>

          val linearray  = line.split("\t")
          if (linearray.length>4)
            {
              val duid = linearray(0)
              val ip = linearray(1)
              val gaid = linearray(3)


              val aid  = linearray(4)

              if (gaid == "" || aid ==""){
                None
              }
              else{
                Some((duid,( ip,gaid.trim,aid.trim)))
              }
            }
          else{
            None
          }

      }
      .mapPartitions{rows =>

      val geoIp = MaxMindIpGeo("GeoIP2-City.mmdb", 1000)

      rows.map{ case (duid,(netIP,gaid,aid))=>


        val location = geoIp.getLocation(netIP)

        var countryCode = ""


        location match {
          case Some(x) =>
            val ipl = x
            ipl.countryCode match {
              case Some(x1) => countryCode = x1
              case None =>
            }

          case None =>
        }
        (duid, (countryCode, gaid, aid))
      }
    }

     userinfor
  }



  def AwsData2process (sc:SparkContext,path:String)={


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
      .flatMap{case line=>

        val linearray = line.split("\t")

        try {

          // val jarray = linearray(6)
          val jobject = new JSONObject(linearray(6))

          val id = linearray(0)

          val key_words = jobject.optString("key_words")
          val hot_words = jobject.optString("hot_words")

          if (key_words!="" && key_words!="Compras"){
            Some ((id, key_words))
          }
          else if (hot_words!=""){
            Some ((id, hot_words))
          }
          else{
            None
          }
        }
        catch{
          case _: Throwable =>

            None
        }

      }

      adlog
  }


  }
