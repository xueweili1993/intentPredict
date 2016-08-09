package test

import java.sql.DriverManager

import org.apache.spark.mllib.clustering.{LDA, LocalLDAModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xinmei on 16/8/8.
  */
object SQLdata4LDA {


   val TopicNum = 5

  def main (args: Array[String])={

    val conf = new SparkConf()

    val sc = new SparkContext(conf)
    val hadoopConf = sc.hadoopConfiguration

    val catePath = "hdfs:///lxw/test3/part-00005"

    val stoppath = "hdfs:///lxw/stopwords"

    val sourcepath = "hdfs:///lxw/ldaData/part*/part-00000"
    val descPath = "hdfs:///lxw/AppWithDiscreption/part-00000"

    val savepath = "hdfs:///lxw/test"

    HDFS.removeFile(savepath)


    val wordtablePath = "hdfs:///lxw/test1"

    val wordTable = sc.textFile(wordtablePath)
      .flatMap{case line=>

        val linearray = line.split(":")
        if (linearray.length>1)
        {
          Some((linearray(0),linearray(1).toInt))
        }
        else {
          None
        }
      }
      // .saveAsTextFile(savepath)
      .collect()

    val length = wordTable.length
    val wordTable1 = wordTable
      .toMap
    val wordTable2 = wordTable.map(_.swap)
      .toMap

    val broadwordTable = sc.broadcast(wordTable1)

    val trainData  = sc.textFile(sourcepath)
      .map{case line =>

          val linearray = line.split("\t")
          val category = linearray(0)
          val words = linearray(1).split(" ")
        (category,words)
      }
      .zipWithIndex()


    val index2them = trainData.map{case ((cate, words),index)=>

      (index,cate)
    }
      .collect()
      .toMap

    val corpus  = trainData
      .flatMap{case ((cate, words),index)=>

        words.map{x =>

          ((index,x),1)

        }
      }
      .reduceByKey(_+_)
      .map{case ((index,word),num)=>

        (index, (word,num))
      }
      .groupByKey()
      .map{case (index,words) =>


        val words_table  = broadwordTable.value
        val indexA = new ArrayBuffer[Int]()
        val freA  = new ArrayBuffer[Double]()

        words.foreach(x=>
          if (words_table.contains(x._1)){

            val freq = x._2
            val index = words_table.get(x._1) match{

              case Some(x) => x
              case None => -1

            }
            if (index!= -1) {
              indexA.append(index)
              freA.append(freq)
            }
          }

        )

        val Vec = Vectors.sparse(length, indexA.toArray,freA.toArray)
        (index, Vec)

      }

    val ldaModel = new LDA()
      .setOptimizer("online")
      .setK(TopicNum)
      .run(corpus)


    HDFS.removeFile("hdfs:///lxw/ldamodel")
    ldaModel.save(sc,"hdfs:///lxw/ldamodel")

    val sameModel = LocalLDAModel.load(sc, "hdfs:///lxw/ldamodel")


    val distribution  = sameModel.topicDistributions(corpus)
      .map{case (index, vec)=>

        var ind = -1
        var weight = 0.0

        for (i<- 0 to TopicNum-1){

          if (vec(i)>0.5){

            ind = i
            weight = vec(i)

          }
        }
        val cate = index2them.get(index) match{
          case Some (x)=> x
          case None => ""
        }

        (cate, ind, weight)
      }
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
