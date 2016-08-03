package test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xinmei on 16/8/3.
  */
object Prepare4LDA {

  def main (args: Array[String])={

    val conf = new SparkConf()

    val sc = new SparkContext(conf)
    val hadoopConf = sc.hadoopConfiguration

    val catePath = "hdfs:///lxw/AppWithCate/part-00000"
    val descPath = "hdfs:///lxw/AppWithDiscreption/part-00000"
    val stoppath = "hdfs:///lxw/stopwords"

    val savepath = "hdfs:///lxw/test1"
    HDFS.removeFile(savepath)

    val stopwords = sc.textFile(stoppath)
      .collect()
      .toSet

    val stopArray = Array("http","facebook","mobil","download","featur","internet","free","video","android")

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

          if (stop.contains(x1)||stopArray.contains(x1))
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




    val wordTable  = AppWithDesc.map(x=>
      x._2)
      .distinct()
      //.collect()
      .zipWithIndex
      .map{case (word,index)=>

         word+":"+index
      }
      .saveAsTextFile(savepath)

   // val length = wordTable.length
    val wordTable1 = wordTable

   // val wordTable2 = wordTable.map(_.swap)


  }

}
