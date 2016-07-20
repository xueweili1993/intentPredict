package test

import scala.collection.mutable.ArrayBuffer
import scala.math._

/**
  * Created by xinmei on 16/7/14.
  */
object StringCompare {

  def main (args: Array[String])={

    val target = "pics PicsArt Photo Studio pics PicsArt Photo Studio"
    val pattern  = "photo studio"
    val k = 5

    val iscontain= fuzzymatch(target, pattern, k)
    println (iscontain)

  }

  def fuzzymatch (target: String, pattern: String, k:Int)={

    val pl = pattern.length
    val tl = target.length


    val B = new ArrayBuffer[(Char,Int)]()
    for (i <- 0 to pl-1){
      val pat = pattern.charAt(i)

      val indexarray = getindex(pattern, pat)
      B += ((pat,Integer.valueOf(indexarray.toString(),2)))

    }

    val Bmap = B.toMap



    val R = new Array[Int](k+1)
    for (i<- 0 to k){

      R(i) = pow(2.0,i).toInt-1
    }


    var sign = false
    //var pos = ""


    for (position<- 0 to tl-1) {

      val targetchar = target.charAt(position)

      val getstatus =

        Bmap.get(targetchar) match {
          case Some(x) => x
          case None => 0

        }

      var Rold = R(0)
      var Rnew = ((Rold << 1) | 1) & getstatus
      R(0) = Rnew

      for (i<- 1 to k){

        Rnew = ((R(i) << 1) & getstatus) | Rold | ((Rold | Rnew) << 1) | 1

        Rold = R(i)
        R(i) = Rnew
      }



      if((pow(2,pl-1).toInt & Rnew )!=0){

        println ("yes"+" "+ targetchar)

        sign = true
        //pos = target.substring(max(0,position-pl+1), position+1)

      }
    }
    sign

  }


  def getindex(Str:String, C:Char)={

       val chararray = Str.toCharArray

       val length = Str.length

       val label = new StringBuilder
       for(i<- 0 to length-1)
         {

           if (chararray(length-1-i)==C){
             label.append("1")
           }
           else{
             label.append("0")
           }
         }

    label
  }

  def showarray (Atoshow: Array[(Int)]): Unit ={

    for (x<- Atoshow){

      val xx =Integer.toBinaryString(x)

      print(xx)

      print("\n")
    }
  }

}
