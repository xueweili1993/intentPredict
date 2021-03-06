package test

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Comparator}

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

    val mmdbPath = "/home/gaoyuan/userLabel/resource/GeoIP2-City.mmdb"
    sc.addFile(mmdbPath)



    val hadoopConf = sc.hadoopConfiguration


    val awsAccessKeyId = args(0)
    val awsSecretAccessKey = args(1)

    val anchordate = "20160426"

    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    hadoopConf.set("fs.s3n.awsAccessKeyId", awsAccessKeyId)

    hadoopConf.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey)






    val caltoday = Calendar.getInstance()
    caltoday.add(Calendar.DATE, -2)
    val date = new SimpleDateFormat("yyyyMMdd").format(caltoday.getTime())
    val hdfspath = "hdfs:///lxw/fuzzymatch/" + date + "/*"
    val savepath = "hdfs:///lxw/fuzzymatchUpdate/" + date
    HDFS.removeFile(savepath)

     getdata.GetHistoryData(sc, date)
     Fuzzymatch(sc, hdfspath, savepath)


    val path  = "hdfs:///lxw/awsdata"
    HDFS.removeFile(path)

    sc.textFile(makepath())
      .saveAsTextFile(path)

    caltoday.add(Calendar.DATE, -27)
    val date1 = new SimpleDateFormat("yyyyMMdd").format(caltoday.getTime())

    val deletepath1 = "hdfs:///lxw/fuzzymatch/" + date1
    HDFS.removeFile(deletepath1)
    val deletepath2 = "hdfs:///lxw/fuzzymatchUpdate/" + date1
    HDFS.removeFile(deletepath2)

  }



  def Fuzzymatch (sc: SparkContext,hdfspath:String, savepath: String) =
  {
    val title = TitleWithCountryAdid(sc)

      .map{case (title, id, payout,country) =>

        val newtitle  =  title.replaceAll("[^a-z]"," ").replaceAll(" +"," ").trim
        (newtitle, id, payout,country)
      }
      .map{case (newtitle, id, payout,country)=>
        val linearray = newtitle.split(" ")
        if (linearray.length>3)
        {
          var ll = ""
          for (i<-0 to 2)
          {
            ll = ll+ " "+linearray(i)
          }
          ((ll.trim,country),(id,payout))
        }
        else {
          ((newtitle.trim,country),(id,payout))
        }
      }
      .filter{case ((newtitle,country),(id,payout)) =>

        // val linearray = newtitle.split(" ")
        newtitle.length<31 && newtitle.length>4
      }
      .groupByKey
      .map{case (title, adlist)=>

        val newsequence = adlist.toArray.sortWith(_._2>_._2)(0)
        (title._1,(title._2,newsequence._1))//(title,country,adid)
      }
      .groupByKey
      //.saveAsTextFile(savepath)
      .collect
      .toSet



    val broadtitle = sc.broadcast(title)


    val mydata = sc.textFile(hdfspath)
      .flatMap {case line =>

        val linearray = line.split("\t")
        if (linearray.length>2) {
          Some((linearray(0), linearray(1),linearray(2)))
        }else{
          None
        }

      }
      .repartition(600)

      .flatMap { case (id, countryCode, textwords) =>

        val titles = broadtitle.value
        val adidlist = new ArrayBuffer[(String, String)]()

        titles.map { case (pattern, iter) =>

          val country2adid = iter.toMap
          val falsebit = {
            if (pattern.length < 10)
              1
            else
              2
          }
          val sign = StringCompare.fuzzymatch(textwords, pattern, falsebit)


          if (sign) {
            if (country2adid.contains(countryCode)) {

              val adid = country2adid.get(countryCode) match {
                case Some(x) => x
                case None => ""
              }

              adidlist += ((adid, pattern))
            }

          }

        }
        val newlist = adidlist.toArray.sortWith(_._2.length > _._2.length)

        if (newlist.length>2){

          Some(id + "_lite_trends_picks_apps"+"\t"+countryCode+"\t"+newlist.mkString("::"))
        }
        else{
          None
        }

      }

      .repartition(1)
      .saveAsTextFile(savepath)

  }


  def TitleWithCountryAdid(sc: SparkContext)={


    val sqlcmd = "SELECT ad.title,ad.id,ad.payout,ad_country.country FROM ad,ad_country WHERE is_deleted = 0 AND agency_name in ('cheetah','taptica','direct','ironsource','youappi') AND can_preload in (1,2) AND (remaining_daily_cap = 0 OR remaining_daily_cap > 30) AND platform = 'android' AND ad.id = ad_country.ad_id"

    val conn = DriverManager.getConnection("jdbc:mysql://172.31.27.7/koala","aduser3", "VbhaYja_eErJ")

    if (!conn.isClosed())
    {
      println("\tSucceeded connecting to the Database!\n")
    }

    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(sqlcmd)

    val adinfo = new ArrayBuffer[(String, String,Double, String)]()

    while(rs.next){
      val title = rs.getString(1).toLowerCase()
      val adid = rs.getString(2)
      val payout = rs.getString(3).toDouble
      val country = rs.getString(4)

      adinfo += ((title, adid, payout, country))
    }


    val rddadinfo = sc.parallelize(adinfo)



    rs.close()
    stmt.close()
    conn.close()

    rddadinfo
  }

  def save2redis(user2adlist:Array[(String,Array[String])])={

    val jedis = new Jedis("xinmei-ad-ec-redis0.ujh2od.0001.usw2.cache.amazonaws.com")
    val p = jedis.pipelined()
    for(item<- user2adlist){
      val adidlist = STRATEGY + "::" +  item._2.mkString(",")

      p.setex(item._1, REDISTTL,adidlist)


      /*println("lxw-log id " + item._1)
      println("gyy-log adid " + adidlist)*/

    }
    p.sync();//这段代码获取所有的response

    p.close()
    jedis.close()

  }


  def makepath()={

    val allpath = new ArrayBuffer[String]()

    val caltoday = Calendar.getInstance()
    caltoday.add(Calendar.DATE, -1)

    for (i<- 1 to 27) {
      //val caltoday = Calendar.getInstance()
      caltoday.add(Calendar.DATE, -1)
      val date = new SimpleDateFormat("yyyyMMdd").format(caltoday.getTime())
      val tempath= "hdfs:///lxw/fuzzymatchUpdate/"+date

      if (HDFS.existFile(tempath)) {
        val tempath1 = tempath+"/*"
        allpath += tempath1
      }


    }
    allpath.toArray.mkString(",")
  }



}
