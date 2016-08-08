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

    val catePath = "hdfs:///lxw/test3/part-00002"

    val stoppath = "hdfs:///lxw/stopwords"

    val savepath = "hdfs:///lxw/ldaData/part2"
    val descPath = "hdfs:///lxw/AppWithDiscreption/part-00000"

    HDFS.removeFile(savepath)


    val stopwords = sc.textFile(stoppath)
      .collect()
      .toSet

    val bStop = sc.broadcast(stopwords)



    val AppWithCate = sc.textFile(catePath)
      .flatMap{case line=>
        try {
          val linearray = line.split("\t")
          val appId = linearray(0)
          val category = linearray(1)

            Some((appId, category))

        }
        catch{
          case _: Throwable =>
            None
        }
      }

    val AppWithDesc = sc.textFile(descPath)

      .map{case line =>

        val linearray  = line.split("\t")
        val appId = linearray(0)
        val  text = {
          if (linearray.length>1)
            linearray(1).toLowerCase().replaceAll("[^a-z]"," ").replaceAll(" +"," ").trim
          else
            ""
        }
        (appId,text)
      }
      .join(AppWithCate)
      .repartition(500)
      .flatMap{case (appId, (text,cate))=>

        val stop  = bStop.value
        val words = text.split(" ")

        words.map{x=>

          var stemmer = new Stemmer()
          stemmer.add(x.trim())
          if ( stemmer.b.length > 2 )
          {
            stemmer.step1()
            stemmer.step2()
            stemmer.step3()
            stemmer.step4()
            stemmer.step5a()
            stemmer.step5b()
          }
          val x1 = stemmer.b

          if (stop.contains(x1))
          {
            (("",""),"")
          }
          else{
            ((appId,cate),x1)
          }
        }

      }
      .filter{case ((appId,cate),word)=>

        word.length>3
      }
      .map{case ((appId,cate),word)=>

        (cate, word)
      }
      .reduceByKey(_+" "+_)
      .map{case (cate, text )=>

          cate+"\t"+text
      }
        .repartition(1)
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

    val adinfo = new ArrayBuffer[(String, String)]()

    while (rs.next) {
      val app_id = rs.getString(1).toLowerCase()
      val category = rs.getString(2)


      adinfo += ((app_id,category))
    }


    val rddadinfo = sc.parallelize(adinfo)


    rs.close()
    stmt.close()
    conn.close()

    rddadinfo
  }

}
