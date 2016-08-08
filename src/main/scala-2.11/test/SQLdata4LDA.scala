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


    val sqlcmd = "select app_id, category from app where is_updated = 1 and platform = 'ANDROID'"

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
