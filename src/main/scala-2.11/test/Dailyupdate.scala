package test

import java.sql.DriverManager

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xinmei on 16/7/26.
  */
object Dailyupdate {

  def main (args: Array[String])= {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)


    val hadoopConf = sc.hadoopConfiguration


    val path = "hdfs:///lxw/awsdata"

    val savepath  = "hdfs:///lxw/test3"

    HDFS.removeFile(savepath)

    val Recommodation = sc.textFile(path)
      .flatMap{case line=>

        val id = line.substring(1,77)
        val country = line.substring(79,81)

        val newstring = line.substring(82, line.length)
        val linearray = newstring.split("\t")

          linearray.map{x=>

            val pair = x.replaceAll("\\(|\\)","").split(",")
            val adid = pair(0)
            val title = pair(1)

            (id, country, adid, title)
          }

      }
      //.distinct()
      .saveAsTextFile(savepath)






  }

  def findadpack(sc:SparkContext, myset:String)={

    val sqlContext = new SQLContext(sc)
    val jdbcDF = sqlContext.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://172.31.27.7:3306/koala?user=aduser3&password=VbhaYja_eErJ",
        "dbtable" -> "ad",
        "driver" -> "com.mysql.jdbc.Driver"
      )
    ).load()

    jdbcDF.registerTempTable("ad")

    val sqlcmd = "select id, title, from ad where id in "+myset
    //val sqlcmd = "select app_id from app"
    val jdbc = jdbcDF.sqlContext.sql(sqlcmd)
      .map{x =>
        (x(0).toString,x(1).toString)
      }

    jdbc

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


}
