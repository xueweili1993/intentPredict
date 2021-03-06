package test

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xinmei on 16/7/26.
  */
object Dailyupdate {


  val REDISTTL = 24*3600 //1 day
  val STRATEGY = "FM"

  def main(args: Array[String]) = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)


    val hadoopConf = sc.hadoopConfiguration


    val path = "hdfs:///lxw/awsdata"

    val savepath = "hdfs:///lxw/awsdata"



    //=================== get all available  title ==================
    val title = TitleWithCountryAdid(sc)
      .map { case (title, id, payout, country) =>

        val newtitle = title.replaceAll("[^a-z]", " ").replaceAll(" +", " ").trim
        (newtitle, id, payout, country)
      }
      .map { case (newtitle, id, payout, country) =>
        val linearray = newtitle.split(" ")
        if (linearray.length > 3) {
          var ll = ""
          for (i <- 0 to 2) {
            ll = ll + " " + linearray(i)
          }
          ((ll.trim, country), (id, payout))
        }
        else {
          ((newtitle.trim, country), (id, payout))
        }
      }
      .filter { case ((newtitle, country), (id, payout)) =>

        newtitle.length < 31 && newtitle.length > 4
      }
      .groupByKey
      .map { case (title, adlist) =>

        val newsequence = adlist.toArray.sortWith(_._2 > _._2)(0)
        (title._1, (title._2, newsequence._1)) //(title,country,adid)
      }
      .groupByKey
      .collect
      .toSet

    val broadtitle = sc.broadcast(title)

    //============get the deleted title ======================

    val Recommodation = sc.textFile(path)
      .flatMap { case line =>

        val Rawarray = line.split("\t")

        val id = Rawarray(0)
        val country = Rawarray(1)


        val linearray = Rawarray(2).split("::")
        linearray.map { x =>

          val pair = x.replaceAll("\\(|\\)", "").split(",")
          val adid = pair(0)
          val title = pair(1)

          (adid, (country, id, title))
        }
      }
      //.saveAsTextFile(savepath)
      .cache()

    val oldAdidList = Recommodation
      .map{case (adid, (country, id, title))=>

        ((id,country), (adid,title))
      }
      .groupByKey()
      .map{case ((id,country), iter)=>

        //(id, country+"\t"+iter.toArray.mkString("::"))
        (id, (country,iter.toArray))
      }


    val idset = Recommodation
      .map { case (adid, (country, id, title)) =>

        adid
      }
      .distinct()
      .collect()
      .mkString(",")

    val ids = "(" + idset + ")"
    println("lxw log"+ ids)


    //============search from sql database=================

    val updatelist = findadpack(sc, ids)

      .join(Recommodation)
      .map { case (adid, (_, (country, id, delete_title))) =>

        ((delete_title, country), id)

      }
      .groupByKey() //((title, country), gaid_aid_oid

      .map { case ((delete_title, country), iditer) =>

         val titles = broadtitle.value
         val adidlist = new ArrayBuffer[(String, String)]()

         titles.map { case (pattern, iter) =>

            val country2adid = iter.toMap
            val falsebit = {
            if (pattern.length < 10)
              2
             else
              4
        }
        val sign = StringCompare.fuzzymatch(delete_title, pattern, falsebit)


        if (sign) {
          if (country2adid.contains(country) && delete_title != pattern) {

            val adid = country2adid.get(country) match {
              case Some(x) => x
              case None => ""
            }

            adidlist += ((adid, pattern))
          }

        }

      }
      val newlist = adidlist.toArray.sortWith(_._2.length > _._2.length)

      (iditer, newlist)

      }
        .filter{case (iditer, newlist)=>

            newlist.nonEmpty
        }
        .flatMap{case (iditer, newlist)=>


            iditer.map{x=>

              (x,newlist)
            }
        }


    val finallist = updatelist
      .fullOuterJoin(oldAdidList)
      .map{case (id,(newlist,oldlist))=>

        val newList = newlist match{

          case Some(x)=> x
          case None => new ArrayBuffer[(String, String)].toArray
        }

        val oldList = oldlist match {

          case Some(x)=> x
          case None => ("",new ArrayBuffer[(String, String)].toArray)
        }

        val country = oldList._1

        (id, country, (newList++oldList._2).take(3))
      }

    HDFS.removeFile(savepath)

        finallist
        .map{ case (id, country, list)=>

          id+"\t"+country+"\t"+list.mkString("::")

         }
        .saveAsTextFile(savepath)

    val saveredisData = finallist
      .map{case (id, country,newList) =>

        (id, newList.map(x=> x._1))

    }
      .collect()

    save2redis(saveredisData)



  }

  def findadpack(sc: SparkContext, myset: String) = {

    val sqlContext = new SQLContext(sc)
    val jdbcDF = sqlContext.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://172.31.27.7:3306/koala?user=aduser3&password=VbhaYja_eErJ",
        "dbtable" -> "ad",
        "driver" -> "com.mysql.jdbc.Driver"
      )
    ).load()

    jdbcDF.registerTempTable("ad")

    val sqlcmd = "select id from ad where id in " + myset + "and is_deleted = 1"
    //val sqlcmd = "select app_id from app"
    val jdbc = jdbcDF.sqlContext.sql(sqlcmd)
      .map { x =>
        (x(0).toString, "")
      }

    jdbc

  }


  def TitleWithCountryAdid(sc: SparkContext) = {


    val sqlcmd = "SELECT ad.title,ad.id,ad.payout,ad_country.country FROM ad,ad_country WHERE is_deleted = 0 AND agency_name in ('cheetah','taptica','direct','ironsource','youappi') AND can_preload in (1,2) AND (remaining_daily_cap = 0 OR remaining_daily_cap > 30) AND platform = 'android' AND ad.id = ad_country.ad_id"

    val conn = DriverManager.getConnection("jdbc:mysql://172.31.27.7/koala", "aduser3", "VbhaYja_eErJ")

    if (!conn.isClosed()) {
      println("\tSucceeded connecting to the Database!\n")
    }

    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(sqlcmd)

    val adinfo = new ArrayBuffer[(String, String, Double, String)]()

    while (rs.next) {
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


}
