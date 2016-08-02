package test

import org.apache.spark.{SparkConf, SparkContext}

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
      .repartition(1000)
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
            ("","")
          }
          else{
            ((appId,cate),x1)
          }
        }

      }
      .reduceByKey(_+" "+_)
      .map{case ((appId,cate),text)=>

        (cate,1)
      }
      .reduceByKey(_+_)
      .saveAsTextFile(savepath)




  }

}
