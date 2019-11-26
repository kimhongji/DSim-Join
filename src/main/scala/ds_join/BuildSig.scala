package ds_join

import com.mongodb.spark._
import com.mongodb.spark.config._
import com.mongodb.spark.sql._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partition, TaskContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import scala.collection.mutable.{ArrayBuffer, Map}

import org.json4s.native.JsonMethods._
import org.json4s.JsonDSL.WithDouble._

import org.apache.spark.sql.SparkSession

import java.io.File
import java.io.PrintWriter
import scala.io.Source

import org.bson.Document


object BuildSig{
  var minimum_f:Int = 0

  def getMin()= {
    minimum_f
  }
    def sort(xs: Array[String]): Array[String] = {
    if (xs.length <= 1) {
      xs
    } else {
      val pivot = xs(xs.length / 2)
      Array.concat(
        sort(xs filter (pivot >)),
        xs filter (pivot ==),
        sort(xs filter (pivot <))
      )
    }
  }

  def sortByValue(x: String): String = {
    sort(x.split(" ")).reduce(_ + " " + _)
  }

  def CalculateH1 ( l: Int, threshold: Double ): Int = {
    // 生成分段的段数(按照query长度)
    Math.floor ( (1 - threshold) * l / threshold + 0.0001).toInt + 1
  }

  def segNum(s: String, n: Int): Int = {
    val hash = s.hashCode % n
    //println(s"segNum: " + s.toString + " , " + hash.toString)
    if (hash >= 0) {
      hash + 1
    } else {
      hash + n + 1
    }
  }

  def createInverse(ss1: String,
                                 group: Array[(Int, Int)],
                                 threshold: Double
                                ): Array[(String, Int, Int)] = {
    {
      val ss = ss1.split(" ").filter(x => x.length > 0)
      val range = group.filter(
        x => (x._1 <= ss.length && x._2 >= ss.length)
      )
      //println(s"createInverse: " + ss1.toString+" / "+  range.length.toString+" /END")
      val sl = range(range.length-1)._1
      val H = CalculateH1(sl, threshold)
      //println(s"createInverse: H: " + H.toString)

      for (i <- 1 until H + 1) yield {
        val s = ss.filter(x => {segNum(x, H) == i})
        if (s.length == 0) {
          Tuple3("", i, sl)
        } else if (s.length == 1) {
          Tuple3(s(0), i, sl)
        } else {
          Tuple3(s.reduce(_ + " " + _), i, sl)
        }
      }
    }.toArray
  }

 /*============= Main function *******************===============*/
	def main(
            sc : org.apache.spark.SparkContext, 
            data: org.apache.spark.rdd.RDD[(String, String)],
            numPartitions:Int):(
                  //org.apache.spark.rdd.RDD[(Int, ((Int, String, Array[(Array[Int], Array[Boolean])]), Boolean))],
                  org.apache.spark.rdd.RDD[(Int, ((String, String), Boolean))],
                  org.apache.spark.rdd.RDD[((Int, Boolean), Long)],
                  Broadcast[Array[(Int, Int)]],
                  org.apache.spark.SparkContext,
                  Int
                  ) ={ 

  //val numPartitions = 4
  val threshold:Double = 0.8  // threshold!!!!!!!
  val alpha = 0.95
  var startTime_2:Double = 0
  var endTime_2:Double = 0
  var index: org.apache.spark.rdd.RDD[(Int,((String, String),Boolean))] = null
  var f: org.apache.spark.rdd.RDD[((Int, Boolean), Long)] = null
  var multiGroup:Broadcast[Array[(Int, Int)]] = null
  //var minimum:Broadcast[Int] = null

  //var conf = new SparkConf().setAppName("BuildSig")
  //var sc = new SparkContext(conf)

  

  def multigroup(mini: Int,
                 maxi: Int,
                 threshold: Double,
                 alpha : Double): Array[(Int, Int)] = {
    var result = ArrayBuffer[(Int, Int)]()
    var l = mini
    while (l <= maxi) {
      val l1 = Math.floor(l / alpha + 0.0001).toInt
      result += Tuple2(l, l1)
      l = l1 + 1
    }
    result.toArray
  }

  def CalculateH ( l: Int, s: Int, threshold: Double ) = {
    Math.floor((1 - threshold) * (l + s) / (1 + threshold) + 0.0001).toInt + 1
  }
/*
  def CalculateH1 ( l: Int, threshold: Double ): Int = {
    // 生成分段的段数(按照query长度)
    Math.floor ( (1 - threshold) * l / threshold + 0.0001).toInt + 1
  }

  def segNum(s: String, n: Int): Int = {
    val hash = s.hashCode % n
    //println(s"segNum: " + s.toString + " , " + hash.toString)
    if (hash >= 0) {
      hash + 1
    } else {
      hash + n + 1
    }
  }

  def createInverse(ss1: String,
                                 group: Array[(Int, Int)],
                                 threshold: Double
                                ): Array[(String, Int, Int)] = {
    {
      val ss = ss1.split(" ").filter(x => x.length > 0)
      val range = group.filter(
        x => (x._1 <= ss.length && x._2 >= ss.length)
      )
      //println(s"createInverse: " + ss1.toString+" / "+  range.length.toString+" /END")
      val sl = range(range.length-1)._1
      val H = CalculateH1(sl, threshold)
      //println(s"createInverse: H: " + H.toString)

      for (i <- 1 until H + 1) yield {
        val s = ss.filter(x => {segNum(x, H) == i})
        if (s.length == 0) {
          Tuple3("", i, sl)
        } else if (s.length == 1) {
          Tuple3(s(0), i, sl)
        } else {
          Tuple3(s.reduce(_ + " " + _), i, sl)
        }
      }
    }.toArray
  } 
  */


  def createDeletion(ss1: String): Array[String] = {
    {
      val ss = ss1.split(" ")
      if (ss.length == 1) {
        Array("")
      } else if (ss.length == 2) {
        Array(ss(0), ss(1))
      } else {
        for (s <- 0 until ss.length) yield {
          Array.concat(ss.slice(0, s), ss.slice(s + 1, ss.length)).reduce(_ + " " + _)
        }
      }.toArray
    }
  }

  
/*
  def sort(xs: Array[String]): Array[String] = {
    if (xs.length <= 1) {
      xs
    } else {
      val pivot = xs(xs.length / 2)
      Array.concat(
        sort(xs filter (pivot >)),
        xs filter (pivot ==),
        sort(xs filter (pivot <))
      )
    }
  }

  def sortByValue(x: String): String = {
    sort(x.split(" ")).reduce(_ + " " + _)
  }
  */


  def hashStrategy(key: Int): Int = {
    val code = (key % numPartitions)
    if (code < 0) {
      code + numPartitions
    } else {
      code
    }
  }

 /*============= Main function ===============*/
  def buildIndex() = {

 	/* input random value */


    val dataRDD = data    //cached
    /* 

    FOR INDEX DATA RDD

    */
 	  val rdd = dataRDD.map(x => (x._1.toString.split(" "), x._2))

    val rdd1 = rdd.map(x => x._1.length).persist(StorageLevel.DISK_ONLY)
    val minimum = sc.broadcast(rdd1.min())
    val maximum = sc.broadcast(rdd1.max())
    val count = sc.broadcast(rdd1.count())
    rdd1.unpersist()
    minimum_f = minimum.value
    //val average = rdd1.sum() / count.value

    multiGroup = sc.broadcast(multigroup(minimum.value, maximum.value, threshold, alpha))

    val inverseRDD = dataRDD
      .map(x => {
        //println(s"inverseRDD = dataRDD : " + x._1.toString + " , " + x._2 )
        (sortByValue(x._1.toString),x._2) 
      })

    val splittedRecord = inverseRDD
      .map(x => {
        //println(s"splittedRecord_1 = inverseRDD: (" + x._1 + " , " + x._2  +")" )
        ((x._1, x._2), createInverse(x._1, multiGroup.value, threshold))
      })
      .flatMapValues(x => x)
      .map(x => {
        //println(s"splittedRecord_2 = inverseRDD: (" + x._1 + " , " + x._2._2 + " , " +  x._2._3 + " , " +  x._2._1  +")")
        ((x._1, x._2._2, x._2._3), x._2._1)
      })

    val deletionIndexSig = splittedRecord
      .filter(x => (x._2.length > 0))
      .map(x => (x._1, createDeletion(x._2))) // (1,i,l), deletionSubstring
      .flatMapValues(x => x)
      .map(x => {
        //println(s"deletionIndexSig = splittedRecord: (" + x._2 + ", " + x._1._2 + ", " + x._1._3 + ")   / hashCode : " +  (x._2, x._1._2, x._1._3).hashCode().toString)
        ((x._2, x._1._2, x._1._3).hashCode(), (x._1._1, true))
      })
    // (hashCode, (String, internalrow))

    val segIndexSig = splittedRecord
      .map(x => {
        //println(s"segIndexSig = splittedRecord: (" + x._2 + ", " + x._1._2 + ", " + x._1._3 + ")  / hashCode : " +  (x._2, x._1._2, x._1._3).hashCode().toString)
        ((x._2, x._1._2, x._1._3).hashCode(), (x._1._1, false))
      })

    index = deletionIndexSig.union(segIndexSig)
    var indexCount = index.count()
    println("index count : "+indexCount)

    f = index
      .map(x => {
        //println(s"F = Index: (" + x._1 + ", " + x._2._2 + ", "+ ")" ) //x._2._1 is row data
        ((x._1, x._2._2), 1L)
      })
      .reduceByKey(_ + _)
      .filter(x => x._2 > 1) // we should change 0 to 2
      .persist()


    //f, multiGroup, minimum 
  }

  /*  ============== function call ============== */ 
  startTime_2 = System.currentTimeMillis();
  buildIndex()
  endTime_2 = System.currentTimeMillis();

  println("time|1|Dima-buildIndex: " + (endTime_2 - startTime_2) + " ms")
  
  

  (index, f, multiGroup ,sc, minimum_f)
  }
}
