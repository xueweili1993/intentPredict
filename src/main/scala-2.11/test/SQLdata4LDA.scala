package test

import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xinmei on 16/8/8.
  */
object SQLdata4LDA {




  def main (args: Array[String])={

    val conf = new SparkConf()

    val sc = new SparkContext(conf)
    val hadoopConf = sc.hadoopConfiguration

    val savepath = "hdfs:///lxw/test2"
    HDFS.removeFile(savepath)

    val adDataWithCate = TitleWithCountryAdid(sc)
      .saveAsTextFile(savepath)


  }






  def TitleWithCountryAdid(sc: SparkContext) = {


    val sqlcmd = "SELECT ad.title,ad.id,ad_country.country,ad.category FROM ad,ad_country WHERE is_deleted = 0 AND is_updated = 1 AND (remaining_daily_cap = 0 OR remaining_daily_cap > 30) AND platform = 'android' AND ad.id = ad_country.ad_id"

    val conn = DriverManager.getConnection("jdbc:mysql://172.31.27.7/koala", "aduser3", "VbhaYja_eErJ")

    if (!conn.isClosed()) {
      println("\tSucceeded connecting to the Database!\n")
    }

    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(sqlcmd)

    val adinfo = new ArrayBuffer[(String, String, String,String)]()

    while (rs.next) {
      val title = rs.getString(1).toLowerCase()
      val adid = rs.getString(2)
      val country = rs.getString(3)
      val category = rs.getString(4)

      adinfo += ((title, adid,country,category))
    }


    val rddadinfo = sc.parallelize(adinfo)


    rs.close()
    stmt.close()
    conn.close()

    rddadinfo
  }

}
