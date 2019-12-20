package ds_join

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner
import org.apache.spark.RangePartitioner
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partition, TaskContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import scala.collection.mutable.{ArrayBuffer, Map}

import com.mongodb.spark._
import com.mongodb.spark.config._

import org.apache.spark.sql.SparkSession

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.collection.mutable
import scala.util.{Failure, Success}


/* ===================================
sbt clean assembly
    Main object 
../Dima-master/bin/spark-submit --class ds_join.DimaJoin --master local[4] ./target/scala-2.10/dima-ds_2.10-1.0.jar > log
../Dima-master/bin/spark-submit --class ds_join.DimaJoin --master local[4] ./target/scala-2.10/Dima-DS-assembly-1.0.jar > log
../spark-2.2.3-bin-hadoop2.6/bin/spark-submit --class ds_join.DimaJoin --master local[4] --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/REVIEW.musical?readPreference=primaryPreferred" ./target/scala-2.11/Dima-DS-assembly-1.0.jar 
../spark-2.2.3-bin-hadoop2.6/bin/spark-submit --class ds_join.DimaJoin --master local[4] --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/REVIEW.musical?readPreference=primaryPreferred" --executor-memory 4G --driver-memory 1G ./target/scala-2.11/Dima-DS-assembly-1.0.jar
../spark-2.2.3-bin-hadoop2.6/bin/spark-submit --class ds_join.DimaJoin --master spark://dsl-desktop:7077 ./target/scala-2.11/Dima-DS-assembly-1.0.jar

===================================*/
object DimaJoin2{

  var numPartitions:Int = 8
  val threshold:Double = 0.8  // threshold!!!!!!!
  //var distribute:Array[Long] = Array()
  var distribute = new Array[Long](4)


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
      val sl = range(range.length-1)._1
      val H = CalculateH1(sl, threshold)

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

  def createInverseForquery(ss1: String,
                                 group: Array[(Int, Int)],
                                 threshold: Double
                                ): Array[(String, Int, Int)] = {
    {
      val ss = ss1.split(" ").filter(x => x.length > 0)
      val s = ss.size
      val range = group
        .filter(x => {
          x._1 <= Math.floor(s / threshold).toInt && x._2 >= (Math.ceil(threshold * s) + 0.0001).toInt
        })
      val sl = range(range.length -1)._1
      
      val H = CalculateH1(sl, threshold)

        val substring = {
          for (i <- 1 until H + 1) yield {
            val p = ss.filter(x => segNum(x, H) == i)
            if (p.length == 0) {
              Tuple3("", i, sl)
            } else if(p.length == 1){ 
              Tuple3(p(0), i, sl)
            }else {
              Tuple3(p.reduce(_ + " " + _), i, sl)
            }
          }
        }
        substring
      }.toArray
  }

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
  
  def inverseDel(         xi: String,
                          indexNum: scala.collection.Map[(Int, Boolean), Long],
                          ii: Int,
                          ll: Int,
                          minimum: Int
                        ): Long = {
    var total = 0.toLong
    if (xi.length == 0) {
      return 0.toLong
    }
    for (i <- createDeletion(xi)) {
      val hash = (i, ii, ll).hashCode()
      total = total + indexNum.getOrElse((hash, false), 0.toLong)
    }
    total
  }

  def addToMapForInverseDel(
                                     xi: String,
                                     indexNum: scala.collection.Map[(Int, Boolean), Long],
                                     partitionTable: scala.collection.Map[Int, Int],
                                     ii: Int,
                                     ll: Int,
                                     minimum: Int,
                                     numPartition: Int
                                   ): Array[(Int, Long)] = {
    if (xi.length == 0) {
      return Array[(Int, Long)]()
    }
    var result = ArrayBuffer[(Int, Long)]()
    for (i <- createDeletion(xi)) {
      val hash = (i, ii, ll).hashCode()
      val partition = partitionTable.getOrElse(hash, hashStrategy(hash))
      result += Tuple2(partition, indexNum.getOrElse((hash, false), 0.toLong))
    }
    result.toArray
  }

  def parent(i: Int) = Math.floor(i / 2).toInt

  def left_child(i: Int) = 2 * i

  def right_child(i: Int) = 2 * i + 1

  def compare(x: (Long, Long), y: (Long, Long)): Short = {
    if (x._1 > y._1) {
      1
    } else if (x._1 < y._1) {
      -1
    } else {
      if (x._2 > y._2) {
        1
      } else if (x._2 < y._2) {
        -1
      } else {
        0
      }
    }
  }

  def minHeapify(A: Array[(Int, (Long, Long), Int)],
                         i: Int): Array[(Int, (Long, Long), Int)] = {
    val l = left_child(i)
    val r = right_child(i)
    val AA = A.clone()
    val smallest = {
      if (l <= AA.length && compare(AA(l - 1)._2, AA(i - 1)._2) < 0) {
        if (r <= AA.length && compare(AA(r - 1)._2, AA(l - 1)._2) < 0) {
          r
        } else {
          l
        }
      }
      else {
        if (r <= AA.length && compare(AA(r - 1)._2, AA(i - 1)._2) < 0) {
          r
        } else {
          i
        }
      }
    }
    if (smallest != i) {
      val temp = AA(i - 1)
      AA(i - 1) = AA(smallest - 1)
      AA(smallest - 1) = temp
      minHeapify(AA, smallest)
    } else {
      AA
    }
  }

  def heapExtractMin(
                              A: Array[(Int, (Long, Long), Int)]
                            ): ((Int, (Long, Long), Int), Array[(Int, (Long, Long), Int)]) = {
    val heapSize = A.length
    if (heapSize < 1) {
       println(s"heap underflow")
    }
    val AA = A.clone()
    val min = AA(0)
    AA(0) = AA(heapSize - 1)
    Tuple2(min, minHeapify(AA.slice(0, heapSize - 1), 1))
  }

  def heapIncreaseKey(
                               A: Array[(Int, (Long, Long), Int)],
                               i: Int,
                               key: (Int, (Long, Long), Int)
                             ): Array[(Int, (Long, Long), Int)] = {
    if (compare(key._2, A(i - 1)._2) > 0) {
      println(s"new key is larger than current Key")
    }
    val AA = A.clone()
    AA(i - 1) = key
    var ii = i
    while (ii > 1 && compare(AA(parent(ii) - 1)._2, AA(ii - 1)._2) > 0) {
      val temp = AA(ii - 1)
      AA(ii - 1) = AA(parent(ii) - 1)
      AA(parent(ii) - 1) = temp
      ii = parent(ii)
    }
    AA
  }

  def minHeapInsert(
                             A: Array[(Int, (Long, Long), Int)],
                             key: (Int, (Long, Long), Int)
                           ): Array[(Int, (Long, Long), Int)] = {
    val AA = Array.concat(A, Array(key).map(x => (x._1, (Long.MaxValue, Long.MaxValue), x._3)))
    heapIncreaseKey(AA, AA.length, key)
  }

  def buildMinHeap(
    A: Array[(Int, (Long, Long), Int)]): Array[(Int, (Long, Long), Int)] = {
    var AA = A.clone()
    for (i <- (1 until Math.floor(AA.length / 2).toInt + 1).reverse) {
      AA = minHeapify(AA, i)
    }
    AA
  }

  def hashStrategy(key: Int): Int = {
    val code = (key % numPartitions)
    if (code < 0) {
      code + numPartitions
    } else {
      code
    }
  }
  def calculateVsl2(
    s: Int,
    l: Int,
    indexNum: scala.collection.Map[(Int, Boolean), Long],
    partitionTable: scala.collection.Map[Int, Int],
    substring: Array[String],
    H: Int,
    minimum: Int,
    alpha: Double,
    numPartition: Int,
    topDegree: Int
  ): Array[Int] = {

    val C0 = {
      for (i <- 1 until H + 1) yield {
        0.toLong
      }
    }.toArray
    val C1 = {
      for (i <- 1 until H + 1) yield {
        val key = ((substring(i - 1), i, l).hashCode(), false)
        indexNum.getOrElse(key, 0.toLong)
      }
    }.toArray
    val C2 = {
      for (i <- 1 until H + 1) yield {
        val key = ((substring(i - 1), i, l).hashCode(), true)
        C1(i - 1) +
          indexNum.getOrElse(key, 0.toLong) +
          inverseDel(substring(i - 1), indexNum, i, l, minimum)
      }
    }.toArray

    val addToDistributeWhen1 = {
      for (i <- 1 until H + 1) yield {
        val hash = (substring(i - 1), i, l).hashCode()
        val partition = partitionTable.getOrElse(hash, hashStrategy(hash))
        (partition, indexNum.getOrElse((hash, false), 0.toLong))
      }
    }.toArray

    val addToDistributeWhen2 = {
      for (i <- 1 until H + 1) yield {
        val hash = (substring(i - 1), i, l).hashCode()
        val partition = partitionTable.getOrElse(hash, hashStrategy(hash))
        val x = addToMapForInverseDel(substring(i - 1),
          indexNum,
          partitionTable,
          i,
          l,
          minimum,
          numPartition)
        Array.concat(Array(
          addToDistributeWhen1(i - 1),
          (partition, indexNum.getOrElse((hash, true), 0.toLong))
        ), x)
      }
    }.toArray

    val deata_distribute0 = {
      // 只考虑有变化的reducer的负载
      for (i <- 0 until H) yield {
        // 分配到1之后情况比较单一,只有inverseindex 和 inversequery匹配这一种情况,只会对一个reducer产生影响
        val max = {
          if (addToDistributeWhen1(i)._2 > 0 && topDegree > 0) {
            (distribute(addToDistributeWhen1(i)._1) +
              addToDistributeWhen1(i)._2.toLong) * 0 //weight(0)
          } else {
            0.toLong
          }
        }
        max
      }
    }.toArray

    val deata_distribute1 = {
      // 分配到2
      for (i <- 0 until H) yield {
        val dis = distribute.slice(0, numPartition).clone()
        val change = ArrayBuffer[Int]()
        for (j <- addToDistributeWhen2(i)) {
          dis(j._1) += j._2.toLong
          if (j._2 > 0) {
            change += j._1
          }
        }
        var total = 0.toLong
        for (ii <- 0 until topDegree) {
          var max = 0.toLong
          var maxPos = -1
          var pos = 0
          for (c <- change) {
            if (dis(c) >= max) {
              max = dis(c)
              maxPos = pos
            }
            pos += 1
          }
          if (maxPos >= 0) {
            change.remove(maxPos)
            total += max * ii //weight(ii) * max
          }
        }
        total
      }
    }.toArray

    val deata0 = {
      for (i <- 0 until H) yield {
        Tuple2(deata_distribute0(i), C1(i) - C0(i))
      }
    }.toArray

    val deata1 = {
      for (i <- 0 until H) yield {
        Tuple2(deata_distribute1(i), C2(i) - C1(i))
      }
    }.toArray

    val Hls = CalculateH(Math.floor(l / alpha + 0.0001).toInt, s, threshold)

    val V = { // initialize
      for (i <- 1 until H + 1) yield {
        0
      }
    }.toArray

    var M = buildMinHeap(deata0.zipWithIndex.map(x => (0, x._1, x._2))) // M initialize

    for (j <- 1 until Hls + 1) {
      val MM = heapExtractMin(M)
      M = MM._2
      val pair = MM._1
      V(pair._3) += 1
      if (V(pair._3) == 1) {
        M = minHeapInsert(M, Tuple3(1, deata1(pair._3), pair._3))
      }
    }

    //remove ( about partiton load)

    V
  }

  def partition_r2(
    ss1: String,
    indexNum: Broadcast[scala.collection.Map[(Int, Boolean), Long]], //frequencyTable
    partitionTable: Broadcast[scala.collection.immutable.Map[Int, Int]],
    minimum: Int,
    group: Broadcast[Array[(Int, Int)]],
    threshold: Double,
    alpha: Double,
    partitionNum: Int,
    topDegree: Int): Array[(Array[(Array[Int], Array[Boolean])],
    Array[(Int, Boolean, Array[Boolean], Boolean, Int)])] = {
    var result = ArrayBuffer[(Array[(Array[Int], Array[Boolean])],
      Array[(Int, Boolean, Array[Boolean],
        Boolean,
        Int)])]()


    val ss = ss1.split(" ").filter(x => x.length > 0)
    val s = ss.size
    val range = group.value
      .filter(x => {
        x._1 <= Math.floor(s / threshold).toInt && x._2 >= (Math.ceil(threshold * s) + 0.0001).toInt
      })
    for (lrange <- range) {
      val l = lrange._1
      val isExtend = {
        if (l == range(range.length - 1)._1) {
          false
        }
        else {
          true
        }
      }

      val H = CalculateH1(l, threshold)

      // each segment and its v value of this record
      val records = ArrayBuffer[(Array[Int], Array[Boolean])]()

      val substring = {
        for (i <- 1 until H + 1) yield {
          val p = ss.filter(x => segNum(x, H) == i)
          if (p.length == 0) "" else if (p.length == 1) p(0) else p.reduce(_ + " " + _)
        }
      }.toArray

      //println(s"===>  call calculateVsl")
      
      val V = calculateVsl2(s,
        l,
        indexNum.value,
        partitionTable.value,
        substring,
        H,
        minimum,
        alpha,
        partitionNum,
        topDegree)


     // println(s"V: " + V.mkString(" ")+" / substring : "+substring.mkString(" ")+ " / partitionNum : "+partitionNum.toString)

      for (i <- 1 until H + 1) {
        val p = ss.filter(x => segNum(x, H) == i)
        records += Tuple2(p.map(x => x.hashCode), {
          if (V(i - 1) == 0) Array()
          else if (V(i - 1) == 1) Array(false)
          else Array(true)
        })
      }
      // probe seg/del signatures of this probe record
      var result1 = ArrayBuffer[(Int, Boolean, Array[Boolean], Boolean, Int)]()
      for (i <- 1 until H + 1) {
        var i_sub = substring(i - 1)
        val hash = (i_sub, i, l).hashCode()
        var v_value = V(i - 1) 
        if (v_value == 1) {
          result1 += Tuple5(hash, false, Array(false), isExtend, i)
        }
        else if (v_value == 2) {
          result1 += Tuple5(hash, false, Array(true), isExtend, i)
          if (i_sub.length > 0) {
            for (k <- createDeletion(i_sub)) {
              val hash1 = (k, i, l).hashCode()
              result1 += Tuple5(hash1, true, Array(true), isExtend, i)
            }
          }
        }
      }
      result += Tuple2(records.toArray, result1.toArray)      
    }

    result.toArray
  }

  def calculateVsl(
    s: Int,
    l: Int,
    indexNum: scala.collection.Map[(Int, Boolean), Long],
    partitionTable: scala.collection.Map[Int, Int],
    substring: Array[String],
    H: Int,
    minimum: Int,
    alpha: Double,
    numPartition: Int,
    topDegree: Int
  ): Array[Int] = {

    val C0 = {
      for (i <- 1 until H + 1) yield {
        0.toLong
      }
    }.toArray
    val C1 = {
      for (i <- 1 until H + 1) yield {
        val key = ((substring(i - 1), i, l).hashCode(), false)
        indexNum.getOrElse(key, 0.toLong)
      }
    }.toArray
    val C2 = {
      for (i <- 1 until H + 1) yield {
        val key = ((substring(i - 1), i, l).hashCode(), true)
        C1(i - 1) +
          indexNum.getOrElse(key, 0.toLong) +
          inverseDel(substring(i - 1), indexNum, i, l, minimum)
      }
    }.toArray

    val addToDistributeWhen1 = {
      for (i <- 1 until H + 1) yield {
        val hash = (substring(i - 1), i, l).hashCode()
        val partition = partitionTable.getOrElse(hash, hashStrategy(hash))
        (partition, indexNum.getOrElse((hash, false), 0.toLong))
      }
    }.toArray

    val addToDistributeWhen2 = {
      for (i <- 1 until H + 1) yield {
        val hash = (substring(i - 1), i, l).hashCode()
        val partition = partitionTable.getOrElse(hash, hashStrategy(hash))
        val x = addToMapForInverseDel(substring(i - 1),
          indexNum,
          partitionTable,
          i,
          l,
          minimum,
          numPartition)
        Array.concat(Array(
          addToDistributeWhen1(i - 1),
          (partition, indexNum.getOrElse((hash, true), 0.toLong))
        ), x)
      }
    }.toArray

    val deata_distribute0 = {
      // 只考虑有变化的reducer的负载
      for (i <- 0 until H) yield {
        // 分配到1之后情况比较单一,只有inverseindex 和 inversequery匹配这一种情况,只会对一个reducer产生影响
        0.toLong
      }
    }.toArray

    val deata_distribute1 = {
      // 分配到2
      for (i <- 0 until H) yield {
        val dis = distribute.slice(0, numPartition).clone()
        val change = ArrayBuffer[Int]()
        for (j <- addToDistributeWhen2(i)) {
          dis(j._1) += j._2.toLong
          if (j._2 > 0) {
            change += j._1
          }
        }
        var total = 0.toLong
        for (ii <- 0 until topDegree) {
          var max = 0.toLong
          var maxPos = -1
          var pos = 0
          for (c <- change) {
            if (dis(c) >= max) {
              max = dis(c)
              maxPos = pos
            }
            pos += 1
          }
          if (maxPos >= 0) {
            change.remove(maxPos)
            total += max * ii //weight(ii) * max
          }
        }
        total
      }
    }.toArray

    val deata0 = {
      for (i <- 0 until H) yield {
        Tuple2(deata_distribute0(i), C1(i) - C0(i))
      }
    }.toArray

    val deata1 = {
      for (i <- 0 until H) yield {
        Tuple2(deata_distribute1(i), C2(i) - C1(i))
      }
    }.toArray

    val Hls = CalculateH(Math.floor(l / alpha + 0.0001).toInt, s, threshold)

    val V = { // initialize
      for (i <- 1 until H + 1) yield {
        0
      }
    }.toArray

    var M = buildMinHeap(deata0.zipWithIndex.map(x => (0, x._1, x._2))) // M initialize

    for (j <- 1 until Hls + 1) {
      val MM = heapExtractMin(M)
      M = MM._2
      val pair = MM._1
      V(pair._3) += 1
      if (V(pair._3) == 1) {
        M = minHeapInsert(M, Tuple3(1, deata1(pair._3), pair._3))
      }
    }

    //remove ( abount partiton load)

    V
  }

  def partition_r(
    ss1: String,
    indexNum: Broadcast[scala.collection.Map[(Int, Boolean), Long]], //frequencyTable
    partitionTable: Broadcast[scala.collection.immutable.Map[Int, Int]],
    minimum: Int,
    group: Broadcast[Array[(Int, Int)]],
    threshold: Double,
    alpha: Double,
    partitionNum: Int,
    topDegree: Int): Array[(Array[(Array[Int], Array[Boolean])],
    Array[(Int, Boolean, Array[Boolean], Boolean, Int)])] = {
    var result = ArrayBuffer[(Array[(Array[Int], Array[Boolean])],
      Array[(Int, Boolean, Array[Boolean],
        Boolean,
        Int)])]()


    val ss = ss1.split(" ").filter(x => x.length > 0)
    val s = ss.size
    val range = group.value
      .filter(x => {
        x._1 <= Math.floor(s / threshold).toInt && x._2 >= (Math.ceil(threshold * s) + 0.0001).toInt
      })
    for (lrange <- range) {
      val l = lrange._1
      val isExtend = {
        if (l == range(range.length - 1)._1) {
          false
        }
        else {
          true
        }
      }

      val H = CalculateH1(l, threshold)

      // each segment and its v value of this record
      val records = ArrayBuffer[(Array[Int], Array[Boolean])]()

      val substring = {
        for (i <- 1 until H + 1) yield {
          val p = ss.filter(x => segNum(x, H) == i)
          if (p.length == 0) "" else if (p.length == 1) p(0) else p.reduce(_ + " " + _)
        }
      }.toArray

      //println(s"===>  call calculateVsl")
      
      val V = calculateVsl(s,
        l,
        indexNum.value,
        partitionTable.value,
        substring,
        H,
        minimum,
        alpha,
        partitionNum,
        topDegree)


     // println(s"V: " + V.mkString(" ")+" / substring : "+substring.mkString(" ")+ " / partitionNum : "+partitionNum.toString)

      for (i <- 1 until H + 1) {
        val p = ss.filter(x => segNum(x, H) == i)
        records += Tuple2(p.map(x => x.hashCode), {
          if (V(i - 1) == 0) Array()
          else if (V(i - 1) == 1) Array(false)
          else Array(true)
        })
      }
      // probe seg/del signatures of this probe record
      var result1 = ArrayBuffer[(Int, Boolean, Array[Boolean], Boolean, Int)]()
      for (i <- 1 until H + 1) {
        var i_sub = substring(i - 1)
        val hash = (i_sub, i, l).hashCode()
        var v_value = V(i - 1) 
        if (v_value == 1) {
          result1 += Tuple5(hash, false, Array(false), isExtend, i)
        }
        else if (v_value == 2) {
          result1 += Tuple5(hash, false, Array(true), isExtend, i)
          if (i_sub.length > 0) {
            for (k <- createDeletion(i_sub)) {
              val hash1 = (k, i, l).hashCode()
              result1 += Tuple5(hash1, true, Array(true), isExtend, i)
            }
          }
        }
      }
      result += Tuple2(records.toArray, result1.toArray)      
    }

    result.toArray
  }
  
  def partition_r_all(
    ss1: String,
    indexNum: Broadcast[scala.collection.Map[(Int, Boolean), Long]], //frequencyTable
    partitionTable: Broadcast[scala.collection.immutable.Map[Int, Int]],
    minimum: Int,
    group: Broadcast[Array[(Int, Int)]],
    threshold: Double,
    alpha: Double,
    partitionNum: Int,
    topDegree: Int): Array[(Array[(Array[Int], Array[Boolean])],
    Array[(Int, Boolean, Array[Boolean], Boolean, Int)])] = {
    var result = ArrayBuffer[(Array[(Array[Int], Array[Boolean])],
      Array[(Int, Boolean, Array[Boolean],
        Boolean,
        Int)])]()


    val ss = ss1.split(" ").filter(x => x.length > 0)
    val s = ss.size
    val range = group.value
      .filter(x => {
        x._1 <= Math.floor(s / threshold).toInt && x._2 >= (Math.ceil(threshold * s) + 0.0001).toInt
      })
    for (lrange <- range) {
      val l = lrange._1
      val isExtend = {
        if (l == range(range.length - 1)._1) {
          false
        }
        else {
          true
        }
      }

      val H = CalculateH1(l, threshold)

      // each segment and its v value of this record
      val records = ArrayBuffer[(Array[Int], Array[Boolean])]()

      val substring = {
        for (i <- 1 until H + 1) yield {
          val p = ss.filter(x => segNum(x, H) == i)
          if (p.length == 0) "" else if (p.length == 1) p(0) else p.reduce(_ + " " + _)
        }
      }.toArray

      //println(s"===>  call calculateVsl")
    

     // println(s"V: " + V.mkString(" ")+" / substring : "+substring.mkString(" ")+ " / partitionNum : "+partitionNum.toString)

      for (i <- 1 until H + 1) {
        val p = ss.filter(x => segNum(x, H) == i)
        records += Tuple2(p.map(x => x.hashCode), {
            Array(true)
        })
      }
      // probe seg/del signatures of this probe record
      var result1 = ArrayBuffer[(Int, Boolean, Array[Boolean], Boolean, Int)]()
      for (i <- 1 until H + 1) {
        var i_sub = substring(i - 1)
        val hash = (i_sub, i, l).hashCode()
           result1 += Tuple5(hash, false, Array(false), isExtend, i)
      }
      result += Tuple2(records.toArray, result1.toArray)      
    }

    result.toArray
  }
  def calculateOverlapBound2(t: Float, xl: Int, yl: Int): Int = {
    (Math.ceil((t / (t + 1)) * (xl + yl)) + 0.0001).toInt
  }

  def verify2(x: Array[(Array[Int], Array[Boolean])], // query
                     y0: ((String, String), Boolean), //index
                     threshold: Double,
                     pos: Int, xLength: Int,
                     multiGroup: Broadcast[Array[(Int, Int)]]
                    ): Boolean = {
   // println(s"enter verification, pos: ${pos}, xLength: ${xLength}, yLength: ${yLength}")
    

    var t0 = System.currentTimeMillis
    val y = createInverse(sortByValue(y0._1._1), multiGroup.value, threshold )
                    .map(x => {
                      if(x._1.length > 0){
                      (x._1.split(" ").map(s => s.hashCode), Array[Boolean]())
                       }else {
                      (Array[Int](), Array[Boolean]())
                      }
                   })
    /* y : Array[(Array[Int], Array[Boolean])] */
    val yLength = y.map(x => x._1.length)
      .reduce(_ + _)
    var t1 = System.currentTimeMillis

    println("time|DIMA|verify2(y, yLength): " + (t1 - t0) + " ms")

    val overlap = calculateOverlapBound2(threshold.asInstanceOf[Float], xLength, yLength)

    var currentOverlap = 0
    var currentXLength = 0
    var currentYLength = 0
    for (i <- 0 until x.length) {
      var n = 0
      var m = 0
      var o = 0
      while (n < x(i)._1.length && m < y(i)._1.length) {
        if (x(i)._1(n) == y(i)._1(m)) {
          o += 1
          n += 1
          m += 1
        } else if (x(i)._1(n) < y(i)._1(m)) {
          n += 1
        } else {
          m += 1
        }
      }
      currentOverlap = o + currentOverlap
      currentXLength += x(i)._1.length
      currentYLength += y(i)._1.length
      val diff = x(i)._1.length + y(i)._1.length - o * 2
      val Vx = {
        if (x(i)._2.length == 0) {
          0
        } else if (x(i)._2.length == 1 && !x(i)._2(0)) {
          1
        } else {
          2
        }
      }
      val Vy = {
        if (y(i)._2.length == 0) {
          0
        } else if (y(i)._2.length == 1 && !y(i)._2(0)) {
          1
        } else {
          2
        }
      }
      if (i + 1 < pos) {
        if ( diff < Vx || diff < Vy) {
          //println(s"diff : ${diff}, Vx :${Vx}, Vy : ${Vy}")
          //println(s"i:$i, overlap")

          return false
        }
      }
      if (currentOverlap + Math.min((xLength - currentXLength),
        (yLength - currentYLength)) < overlap) {
        /*
        println(s"i:$i, currentOverlap:$currentOverlap, " +
          s"xLength: $xLength, yLength: $yLength, currentXLength: $currentXLength, " +
          s"currentYLength: $currentYLength, overlap: $overlap, prune")
        */
        return false
      }
    }
    if (currentOverlap >= overlap) {
      return true
    } else {
      //println(s"finalOverlap:$currentOverlap, overlap: $overlap, false")
    
      return false
    }
  }

  def compareSimilarity2(
    query: ((Int, String, Array[(Array[Int], Array[Boolean])])
      , Boolean, Array[Boolean], Boolean, Int),
    index: ((String, String), Boolean),
    multiGroup: Broadcast[Array[(Int, Int)]], 
    threshold: Double): Boolean = {

    val pos = query._5
    val query_length = query._1._3
      .map(x => x._1.length)
      .reduce(_ + _)


    if (index._2) { //
      if (!query._2 && query._3.length > 0 && query._3(0)) {
          verify2(query._1._3, index, threshold, pos,
            query_length, multiGroup)
      } else {
        false
      }
    } else {
      verify2(query._1._3, index, threshold, pos,
        query_length, multiGroup)
    }
  }
  def main(
            sc : org.apache.spark.SparkContext, 
            data: org.apache.spark.rdd.RDD[(Int, ((String, String), Boolean))], 
            //data: org.apache.spark.rdd.RDD[IPartition],
            query: org.apache.spark.rdd.RDD[(String, String)],
            //query: org.apache.spark.rdd.RDD[(Int, ((Int, String, Array[(Array[Int], Array[Boolean])]), Boolean, Array[Boolean], Boolean, Int))],
            frequencyTable: Broadcast[scala.collection.Map[(Int, Boolean), Long]],
            partitionTable: Broadcast[scala.collection.immutable.Map[Int, Int]],
            multiGroup:Broadcast[Array[(Int, Int)]],
            minimum:Int,
            numPartition:Int)
            : (org.apache.spark.rdd.RDD[(Int, String)], org.apache.spark.SparkContext) = { 

  numPartitions = numPartition //default : 2
  var hashP = new HashPartitioner(numPartitions)
  //val threshold:Double = 0.8  // threshold!!!!!!!
  val alpha = 0.95
  val topDegree = 0
  val abandonNum = 2
  val weight = 0
  var partitionNumToBeSent = 1 //defatult : 1
  //val distribute = new Array[Long](2048)
  var indexDistribute = new Array[Long](2048)
  var startTime_2:Double = 0
  var endTime_2:Double = 0
  var est_2:Double = 0
  var dimajoined:org.apache.spark.rdd.RDD[(Int, String)] = null

  //var conf = new SparkConf().setAppName("DimaJoin")
  //var sc = new SparkContext(conf)

  startTime_2 = System.currentTimeMillis();
  dimajoined = buildIndex()
  endTime_2 = System.currentTimeMillis();

  //println("time|2|Dima-buildIndex: " + (endTime_2 - startTime_2) + " ms")

  //est_2 = endTime_2 - startTime_2
  //println("After indexing : " + est_2/1000000000.0)
  /*
    local function location
  */
  def calculateOverlapBound(t: Float, xl: Int, yl: Int): Int = {
    (Math.ceil((t / (t + 1)) * (xl + yl)) + 0.0001).toInt
  }

  def verify(x: Array[(Array[Int], Array[Boolean])], // query
                     y: Array[(Array[Int], Array[Boolean])], //index
                     threshold: Double,
                     pos: Int, xLength: Int, yLength: Int
                    ): Boolean = {
    println(s"enter verification, pos: ${pos}, xLength: ${xLength}, yLength: ${yLength}")
    val overlap = calculateOverlapBound(threshold.asInstanceOf[Float], xLength, yLength)
    var currentOverlap = 0
    var currentXLength = 0
    var currentYLength = 0
    for (i <- 0 until x.length) {
      var n = 0
      var m = 0
      var o = 0
      while (n < x(i)._1.length && m < y(i)._1.length) {
        if (x(i)._1(n) == y(i)._1(m)) {
          o += 1
          n += 1
          m += 1
        } else if (x(i)._1(n) < y(i)._1(m)) {
          n += 1
        } else {
          m += 1
        }
      }
      currentOverlap = o + currentOverlap
      currentXLength += x(i)._1.length
      currentYLength += y(i)._1.length
      val diff = x(i)._1.length + y(i)._1.length - o * 2
      val Vx = {
        if (x(i)._2.length == 0) {
          0
        } else if (x(i)._2.length == 1 && !x(i)._2(0)) {
          1
        } else {
          2
        }
      }
      val Vy = {
        if (y(i)._2.length == 0) {
          0
        } else if (y(i)._2.length == 1 && !y(i)._2(0)) {
          1
        } else {
          2
        }
      }
      if (i + 1 < pos) {
        if ( diff < Vx || diff < Vy) {
          println(s"diff : ${diff}, Vx :${Vx}, Vy : ${Vy}")
          println(s"i:$i, overlap")
          println("false2")
          return false
        }
      }
      if (currentOverlap + Math.min((xLength - currentXLength),
        (yLength - currentYLength)) < overlap) {
        
        println(s"i:$i, currentOverlap:$currentOverlap, " +
          s"xLength: $xLength, yLength: $yLength, currentXLength: $currentXLength, " +
          s"currentYLength: $currentYLength, overlap: $overlap, prune")
          
          println("false3")
        return false
      }
    }
    if (currentOverlap >= overlap) {
      return true
    } else {
      println(s"finalOverlap:$currentOverlap, overlap: $overlap, false")
      println("false4")
      return false
    }
  }

  def compareSimilarity(
    query: ((Int, String, Array[(Array[Int], Array[Boolean])])
      , Boolean, Array[Boolean], Boolean, Int),
    index: ((String, String), Boolean)): Boolean = {

    val pos = query._5
    val query_length = query._1._3
      .map(x => x._1.length)
      .reduce(_ + _)


    val indexArr = createInverse(sortByValue(index._1._1), multiGroup.value, threshold )
                    .map(x => {
                      if(x._1.length > 0){
                      (x._1.split(" ").map(s => s.hashCode), Array[Boolean]())
                       }else {
                      (Array[Int](), Array[Boolean]())
                      }
                   })
    /* indexArr : Array[(Array[Int], Array[Boolean])] */
    val index_length = indexArr.map(x => x._1.length)
      .reduce(_ + _)


    if (index._2) { //
      if (!query._2 && query._3.length > 0 && query._3(0)) {
          verify(query._1._3, indexArr, threshold, pos,
            query_length, index_length)
      } else {
        println("false1")
        false
      }
    } else {
      verify(query._1._3, indexArr, threshold, pos,
        query_length, index_length)
    }
  }  

 /*============= Main function ===============*/
  def buildIndex():org.apache.spark.rdd.RDD[(Int, String)] = {

  /* input random value */
    

    def Has(x : Int, array: Array[Int]): Boolean = {
      for (i <- array) {
        if (x == i) {
           return true
         }
       }
     false
    }  

    val index = data
    val queryRDD = query  //stream

    var indexRDD:org.apache.spark.rdd.RDD[(Int, ((Int, String, Array[(Array[Int], Array[Boolean])]), Boolean))] = null
    var query_rdd_partitioned:ds_join.SimilarityRDD[(Int, ((Int, String, Array[(Array[Int], Array[Boolean])]), Boolean, Array[Boolean], Boolean, Int))] = null
    var maxPartitionId:org.apache.spark.broadcast.Broadcast[Array[Int]] = null
    var query_rdd:org.apache.spark.rdd.RDD[(Int, ((Int, String, Array[(Array[Int], Array[Boolean])]), Boolean, Array[Boolean], Boolean, Int))] = null

  
    var t0 = System.currentTimeMillis();
       
    //println("in DIMA index  : " +index.partitioner)
    //println("in DIMA queryRDD  : " +queryRDD.partitioner)
    val partitionedRDD = index
    //val partitionedRDD = index.partitionBy(new SimilarityHashPartitioner(numPartitions, partitionTable))
    //println("partitionedRDD.partitioner: "+partitionedRDD.partitioner)//SimilarityHashPartitioner
    /*indexRDD = partitionedRDD.mapPartitions({ iter => 
      iter.map( s => (s._1 , ((sortByValue(s._2._1._1).hashCode, s._2._1._2, 
           createInverse(sortByValue(s._2._1._1), multiGroup.value, threshold)
        .map(x => {
          if (x._1.length > 0) {
             (x._1.split(" ").map(s => s.hashCode), Array[Boolean]())
           } else {
            (Array[Int](), Array[Boolean]())
           }
         })), s._2._2)))//.iterator
      }, preservesPartitioning=true).cache()   
    */
      //println("indexRDD.partitioner: "+indexRDD.partitioner)//SimilarityHashPartitioner
     var t1 = System.currentTimeMillis();


    /* 

    FOR QUERY DATA RDD

    */
        //query_rdd = queryRDD
        /*
        query_rdd = queryRDD
          .map(x => (sortByValue(x._1), x._2))
          .map(x => ((x._1.hashCode, x._2, x._1),
             partition_r(
              x._1, frequencyTable, partitionTable, minimum, multiGroup,
             threshold, alpha, numPartitions, topDegree
            )))
          .flatMapValues(x => x)
          .map(x => {
            ((x._1._1, x._1._2, x._2._1), x._2._2)
          })
         .flatMapValues(x => x)
         .map(x => {
            (x._2._1, (x._1, x._2._2, x._2._3, x._2._4, x._2._5))
          }).cache()
    */

     var max_temp = Array(-1)
     query_rdd_partitioned = new SimilarityRDD(
       query_rdd.partitionBy(new SimilarityQueryPartitioner(numPartitions, partitionTable, frequencyTable, max_temp)), true
     )
     var ans2 = mutable.ListBuffer[(Int, String, String)]()
     
     /* index = (signature, ((string, string), bool)) */
     var cogroupCRDD = query_rdd_partitioned.cogroup(index).flatMapValues(pair => for(v <- pair._1.iterator; w <- pair._2.iterator) yield (v, w))
     /* cogroupCRDD = (signature, (iter(q), iter(i))) */
     println("cogroupCRDD.partitioner: "+cogroupCRDD.partitioner)
     var final_result_p = cogroupCRDD.mapPartitions({ iter =>
        while(iter.hasNext){
          var data = iter.next

          var q = data._2._1
          var i = data._2._2
          
          println("q: "+q._1._1+" i: "+i._1._1.hashCode)
          if(compareSimilarity(q, i)){ 
              println("push")
              ans2 += Tuple3(q._1._2.hashCode(), q._1._2, i._1._2) // or q._2._1._2.hashCode()
            }       
          }
        ans2.map(x => (x._1, x._3)).iterator     
      }, preservesPartitioning = true)
     println("final_result_p.partitioner: "+final_result_p.partitioner)
 
    indexRDD.unpersist()
    //query_rdd_partitioned.unpersist()
    query_rdd.unpersist()
   
    final_result_p
    }

    (dimajoined, sc)
  }

}
