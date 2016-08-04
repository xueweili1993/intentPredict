package test

import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA, LocalLDAModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

/**
  * Created by xinmei on 16/8/2.
  */
object LDAtrain {

  val TopicNum =3

  def main (args:Array[String])={

    val conf = new SparkConf()

    val sc = new SparkContext(conf)
    val hadoopConf = sc.hadoopConfiguration

    val catePath = "hdfs:///lxw/AppWithCate/part-00000"
    val descPath = "hdfs:///lxw/AppWithDiscreption/part-00000"
    val stoppath = "hdfs:///lxw/stopwords"

    val savepath = "hdfs:///lxw/test"
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




    /*val wordTable  = AppWithDesc.map(x=>
      x._2)
      .distinct()
      .collect()
      .zipWithIndex

    val length = wordTable.length
    val wordTable1 = wordTable
      .toMap
    val wordTable2 = wordTable.map(_.swap)
      .toMap

    val broadwordTable = sc.broadcast(wordTable1)*/

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





// == id : appId,cate=====
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
              case None => -1

            }
            if (index!= -1) {
              indexA.append(index)
              freA.append(freq)
            }
          }

        )

        val Vec = Vectors.sparse(length, indexA.toArray,freA.toArray)

        (id,Vec)
      }
      .repartition(50)



    val raw = userTable.zipWithIndex
    val corpus  = raw.map(x=> (x._2,x._1._2)).cache()
    val idWPithIndex = raw.map(x=>(x._2,x._1._1._2))
      .collect()
      .toMap


    val ldaModel = new LDA()
      .setOptimizer("online")
      .setK(TopicNum)
      .run(corpus)

//    val topics = ldaModel.topicsMatrix
//
//    val DocTopic  = ldaModel.describeTopics(100)
//
//    val mapp = new HashMap[String,Int]()
//
//    for (topic <- Range(0,TopicNum)){
//
//      val textunit = DocTopic(topic)
//      val textid = textunit._1
//      val length = textid.length
//
//      println ("Topic: "+ topic+ " ")
//
//      for (i<-0 to length-1){
//        val text = wordTable2.get(textid(i)) match {
//          case Some(x)=> x
//          case None => ""
//        }
//        val weight =textunit._2(i)
//
//        if (mapp.contains(text)){
//
//          val count  = mapp.get(text) match{
//            case Some(x)=> x
//            case None=> 0
//
//          }
//          val newcount = count+1
//          mapp.remove(text)
//          mapp.put(text,newcount)
//        }
//        else {
//          mapp.put(text,1)
//        }
//
//        print (text + ":"+ weight+" ")
//      }
//
//    }
//
//    mapp.toArray.sortWith(_._2>_._2)foreach(x=>
//
//      print ("lxw log :" + x+ " ")
//    )



    /*for (topic <- Range(0, 10)) {
      println("Topic " + topic + ":")

      val wordWeight = new ArrayBuffer[(String,Double)]()

      for (word <- Range(0, ldaModel.vocabSize)) {
        val words = wordTable2.get(word) match{
          case Some(x)=> x
          case None => ""
        }
        wordWeight.append((words,topics(word,topic)))
      }
      val sortedarray = wordWeight.toArray.sortWith(_._2>_._2).take(100)
      sortedarray.foreach(x=>

        print(x+",")
      )

    }*/


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
            val cate = idWPithIndex.get(index) match{
              case Some (x)=> x
              case None => ""
            }

          (cate, ind, weight)
        }
      //.saveAsTextFile(savepath)
      .map{case (cate, ind, weight)=>

          ((cate,ind),1)
        }
      .reduceByKey(_+_)
      .map{case ((cate,ind),num)=>

        (cate,(ind,num))
      }
      .groupByKey()
      .map{case (cate,iter)=>

          val numm = iter.toArray.mkString("\t")
        (cate, numm)
      }
      .saveAsTextFile(savepath)




  }

}
