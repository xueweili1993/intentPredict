package test

import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xinmei on 16/8/2.
  */
object LDAtrain {

  def main (args:Array[String])={

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



    bStop.destroy()
    val wordTable  = AppWithDesc.map(x=>
      x._2)
      .distinct()
      .collect()
      .zipWithIndex

    val length = wordTable.length
    val wordTable1 = wordTable
      .toMap
    val wordTable2 = wordTable.map(_.swap)
      .toMap


    val broadwordTable = sc.broadcast(wordTable1)

    val userTable  = AppWithDesc.map{case (id, word)=>

      ((id, word),1)
    }
      .reduceByKey(_+_)
      .map{case ((id,word),num)=>

        (id,(word,num))
      }
      .groupByKey()
      .map{case (id, iter)=>

        val words_table  = broadwordTable.value
        val indexA = new ArrayBuffer[Int]()
        val freA  = new ArrayBuffer[Double]()

        iter.foreach(x=>
          if (words_table.contains(x._1)){

            val freq = x._2
            val index = words_table.get(x._1) match{

              case Some(x) => x
              case None => 0

            }

            indexA.append(index)
            freA.append(freq)
          }

        )

        val Vec = Vectors.sparse(length, indexA.toArray,freA.toArray)

        (id,Vec)
      }

    //broadwordTable.destroy()

    val raw = userTable.zipWithIndex
    val corpus  = raw.map(x=> (x._2,x._1._2)).cache()
    val idWPithIndex = raw.map(x=>(x._2,x._1._1))
      //.collect()


    val ldaModel = new LDA().setK(10).run(corpus)

    val topics = ldaModel.topicsMatrix
    //val broadTopic  =  sc.broadcast(topics)




    for (topic <- Range(0, 10)) {
      print("Topic " + topic + ":")

      val wordWeight = new ArrayBuffer[(String,Double)]()

      for (word <- Range(0, ldaModel.vocabSize)) {
        val words = wordTable2.get(words) match{
          case Some(x)=> x
          case None => ""
        }
        wordWeight.append((words,topics(word,topic)))
      }
      val sortedarray = wordWeight.toArray.sortWith(_._2>_._2)
      sortedarray.foreach(x=>

        print(x+",")
      )

    }


  }

}
