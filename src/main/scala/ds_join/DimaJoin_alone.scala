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

/* ===================================
sbt clean assembly
    Main object 
../Dima-master/bin/spark-submit --class ds_join.DimaJoin --master local[4] ./target/scala-2.10/dima-ds_2.10-1.0.jar > log
../Dima-master/bin/spark-submit --class ds_join.DimaJoin --master local[4] ./target/scala-2.10/Dima-DS-assembly-1.0.jar > log
../spark-2.2.3-bin-hadoop2.6/bin/spark-submit --class ds_join.DimaJoin --master local[4] --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/REVIEW.musical?readPreference=primaryPreferred" ./target/scala-2.11/Dima-DS-assembly-1.0.jar 
../spark-2.2.3-bin-hadoop2.6/bin/spark-submit --class ds_join.DimaJoin --master local[4] --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/REVIEW.musical?readPreference=primaryPreferred" --executor-memory 4G --driver-memory 1G ./target/scala-2.11/Dima-DS-assembly-1.0.jar
../spark-2.2.3-bin-hadoop2.6/bin/spark-submit --class ds_join.DimaJoin --master spark://dsl-desktop:7077 ./target/scala-2.11/Dima-DS-assembly-1.0.jar

===================================*/
object DimaJoin_alone{

  def main(args: Array[String]){

  val table_name = "table-name"
  val index_name = "index-name"
  val numPartitions = 2
  val threshold:Double = 0.8
  val alpha = 0.95
  val topDegree = 0
  val abandonNum = 2
  val weight = 0
  var partitionNumToBeSent = 1
  var startTime_2:Double = 0
  var endTime_2:Double = 0
  var est_2:Double = 0


  val distribute = new Array[Long](2048)
  val indexDistribute = new Array[Long](2048)

  startTime_2 = System.currentTimeMillis();
  buildIndex()
  endTime_2 = System.currentTimeMillis();

  println("time|2|Dima-queryZipPartition: " + (endTime_2 - startTime_2) + " ms")

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

    val V = {
      for (i <- 1 until H + 1) yield {
        0
      }
    }.toArray

    var M = buildMinHeap(deata0.zipWithIndex.map(x => (0, x._1, x._2)))

    for (j <- 1 until Hls + 1) {
      val MM = heapExtractMin(M)
      M = MM._2
      val pair = MM._1
      V(pair._3) += 1
      if (V(pair._3) == 1) {
        M = minHeapInsert(M, Tuple3(1, deata1(pair._3), pair._3))
      }
    }

    for (chooseid <- 0 until H) {
      if (V(chooseid) == 1) {
        distribute(addToDistributeWhen1(chooseid)._1) += addToDistributeWhen1(chooseid)._2.toLong
      } else if (V(chooseid) == 2) {
        for (j <- addToDistributeWhen2(chooseid)) {
          distribute(j._1) += j._2.toLong
        }
      }
    }
    V
  }

  /*================query defenition ===================*/
   def partition_r(
    ss1: String,
    indexNum: Broadcast[scala.collection.Map[(Int, Boolean), Long]],
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

      println(s"=====> call calculateVsl")
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

      //println(s"V: " + V.mkString(" ")+" / substring : "+substring.mkString(" ")+ " / partitionNum : "+partitionNum.toString)

      for (i <- 1 until H + 1) {
        val p = ss.filter(x => segNum(x, H) == i)
        val a = p.map(x => {
          //println(s"records_hashCode : "+x+" , "+x.hashCode.toString)
        })
        records += Tuple2(p.map(x => x.hashCode), {
          if (V(i - 1) == 0) Array()
          else if (V(i - 1) == 1) Array(false)
          else Array(true)
        })
      }
      // probe seg/del signatures of this probe record
      var result1 = ArrayBuffer[(Int, Boolean, Array[Boolean], Boolean, Int)]()
      for (i <- 1 until H + 1) {
        val hash = (substring(i - 1), i, l).hashCode()
        if (V(i - 1) == 1) {
          //println(s"segProbeSig_1: (" + substring(i - 1) + ", " + i + ", " + l + ")" )
          result1 += Tuple5(hash, false, Array(false), isExtend, i)
        }
        else if (V(i - 1) == 2) {
          //println(s"segProbeSig_2: (" + substring(i - 1) + ", " + i + ", " + l + ")" )
          result1 += Tuple5(hash, false, Array(true), isExtend, i)
          if (substring(i - 1).length > 0) {
            for (k <- createDeletion(substring(i - 1))) {
              //println(s"segProbeSig_2: (" + k + ", " + i + ", " + l + ")" )
              val hash1 = (k, i, l).hashCode()
              result1 += Tuple5(hash1, true, Array(true), isExtend, i)
            }
          }
        }
      }
      result += Tuple2(records.toArray, result1.toArray)
      //println(s"partition_r_Tuple"+records.toArray.mkString(" ")+" , "+result1.toArray.mkString(" "))
      //println(s"#partition_r_Tuple_record")
      //for(e <- records) println(s"  >>hash:  "+e._1.mkString(" ")+ ", "+e._2.mkString(" "))
      //println(s"#partition_r_Tuple_result")
      //for(e <- result1) println(s"  >>hash : "+e._1+","+e._3.mkString(" "))
    }

    result.toArray
  }
  

  def calculateOverlapBound(t: Float, xl: Int, yl: Int): Int = {
    (Math.ceil((t / (t + 1)) * (xl + yl)) + 0.0001).toInt
  }

  def verify(x: Array[(Array[Int], Array[Boolean])],
                     y: Array[(Array[Int], Array[Boolean])],
                     threshold: Double,
                     pos: Int, xLength: Int, yLength: Int
                    ): Boolean = {
    //println(s"enter verification, pos: ${pos}, xLength: ${xLength}, yLength: ${yLength}")
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
        if (diff < Vx || diff < Vy) {
          println(s"i:$i, overlap")
          return false
        }
      }
      if (currentOverlap + Math.min((xLength - currentXLength),
        (yLength - currentYLength)) < overlap) {
        println(s"i:$i, currentOverlap:$currentOverlap, " +
          s"xLength: $xLength, yLength: $yLength, currentXLength: $currentXLength, " +
          s"currentYLength: $currentYLength, overlap: $overlap, prune")
        return false
      }
    }
    if (currentOverlap >= overlap) {
      return true
    } else {
     // println(s"finalOverlap:$currentOverlap, overlap: $overlap, false")
      return false
    }
  }

  def compareSimilarity(
    query: ((Int, String, Array[(Array[Int], Array[Boolean])])
      , Boolean, Array[Boolean], Boolean, Int),
    index: ((Int, String, Array[(Array[Int], Array[Boolean])]), Boolean)): Boolean = {
    println(s"compare { ${query._1._2} } and " +
      s"{ ${index._1._2}}")
    println(s"isDeletionIndex: ${index._2}, isDeletionQuery: ${query._2}, val" +
      s"ue: ${
        if (query._3.length == 0) {
          0
        } else if (!query._3(0)) {
          1
        } else {
          2
        }
      }")
    val pos = query._5
    val query_length = query._1._3
      .map(x => x._1.length)
      .reduce(_ + _)
    val index_length = index._1._3
      .map(x => x._1.length)
      .reduce(_ + _)
    if (index._2) { //
      if (!query._2 && query._3.length > 0 && query._3(0)) {
          verify(query._1._3, index._1._3, threshold, pos,
            query_length, index_length)
      } else {
        false
      }
    } else {
      verify(query._1._3, index._1._3, threshold, pos,
        query_length, index_length)
    }
  }
 /*============= Main function ===============*/
  def buildIndex(): Unit = {

  /* input random value */

    
    val conf = new SparkConf().setAppName("DimaJoin_alone")
    val sc = new SparkContext(conf)

    val data_num = args(0).toString
    val coll_name = "mongodb://192.168.0.11:27017/REVIEW.musical_"+data_num
    println("index coll: "+coll_name)
    println("query coll: mongodb://192.168.0.11:27017/REVIEW.musical_100")

    //index collection
    val l1 = "the cable is great for connecting directly to the computer microphone to record live without a console or mixer... its great  i recomended"
    val l2 = "It is a decent cable. It does its, but it leaves much to be desired as far as structural integrity is concerned. The mating part of the connector jiggles back and forth. The switch is also loose. The connector will move around 2mm out of the microphone without the switch being pressed.'I would not recommend using this cable for critical applications. It would be more suited to home or hobby."
    val l3 = "If you're like me, you probably bought this to hook up an XLR microphone to a digital recorder or a PC.It works fine for the handy type recorders.But forget hooking this directly to a PC.  Way too much."
    val l4 = "Haven't used it yet but if it's like the other cables I have from the same company, it should work."
    val l5 = "I use this cable to run a extra line from microphone podiums to a zoom H1. This adapter has never failed me and has been instrumental in capturing quality audio at live events. I own several of these and love them.I highly recommend this adapter to any visual or audio professional. Also check out Hosa's sinlge XLR to double splitter."
    val readConfig = ReadConfig(Map(
      "spark.mongodb.input.uri" -> coll_name,
      "spark.mongodb.input.readPreference.name" -> "primaryPreferred"      
      ))

    //query collection
    val query_readConfig = ReadConfig(Map(
      "spark.mongodb.input.uri" -> "mongodb://192.168.0.11:27017/REVIEW.musical_100", 
      "spark.mongodb.input.readPreference.name" -> "primaryPreferred" 
      ))

    val load = MongoSpark.load(sc,readConfig)
    val preRDD = load.map( x => x.getString("reviewText"))
    val dataRDD = preRDD.map(x => (x,x))

    
    //val row = sc.parallelize(List(l1,l2,l3,l4,l5))
    val query_load = MongoSpark.load(sc,query_readConfig)
    val query_preRDD = query_load.map( x => x.getString("reviewText"))
    val queryRDD = query_preRDD.map(x => (x,x))

    /* 

    FOR INDEX DATA RDD

    */

    val rdd = dataRDD.map(x => (x._1.toString.split(" "), x._2))

    val rdd1 = rdd.map(x => x._1.length).persist(StorageLevel.DISK_ONLY)
    val minimum = sc.broadcast(rdd1.min())
    val maximum = sc.broadcast(rdd1.max())
    val count = sc.broadcast(rdd1.count())
    rdd1.unpersist()
    //val average = rdd1.sum() / count.value

    val multiGroup = sc.broadcast(multigroup(minimum.value, maximum.value, threshold, alpha))

    val inverseRDD = dataRDD
      .map(x => {
       // println(s"inverseRDD = dataRDD : " + x._1.toString + " , " + x._2 )
        (sortByValue(x._1.toString),x._2) 
      })

    val splittedRecord = inverseRDD
      .map(x => {
        println(s"splittedRecord_1 = inverseRDD: (" + x._1 + " , " + x._2  +")" )
        ((x._1, x._2), createInverse(x._1, multiGroup.value, threshold))
      })
      .flatMapValues(x => x)
      .map(x => {
       // println(s"splittedRecord_2 = inverseRDD: (" + x._1 + " , " + x._2._2 + " , " +  x._2._3 + " , " +  x._2._1  +")")
        ((x._1, x._2._2, x._2._3), x._2._1)
      })

    val deletionIndexSig = splittedRecord
      .filter(x => (x._2.length > 0))
      .map(x => (x._1, createDeletion(x._2))) // (1,i,l), deletionSubstring
      .flatMapValues(x => x)
      .map(x => {
      //  println(s"deletionIndexSig = splittedRecord: (" + x._2 + ", " + x._1._2 + ", " + x._1._3 + ")   / hashCode : " +  (x._2, x._1._2, x._1._3).hashCode().toString)
        ((x._2, x._1._2, x._1._3).hashCode(), (x._1._1, true))
      })
    // (hashCode, (String, internalrow))

    val segIndexSig = splittedRecord
      .map(x => {
        //println(s"segIndexSig = splittedRecord: (" + x._2 + ", " + x._1._2 + ", " + x._1._3 + ")  / hashCode : " +  (x._2, x._1._2, x._1._3).hashCode().toString)
        ((x._2, x._1._2, x._1._3).hashCode(), (x._1._1, false))
      })

    val index = deletionIndexSig.union(segIndexSig).persist(StorageLevel.DISK_ONLY)

    val f = index
      .map(x => {
       // println(s"F = Index: (" + x._1 + ", " + x._2._2 + ", "+ ")" ) //x._2._1 is row data
        ((x._1, x._2._2), 1L)
      })
      .reduceByKey(_ + _)
      .filter(x => x._2 > 2) // we should change 0 to 2
      .persist()

    val frequencyTable = sc.broadcast(f.collectAsMap())

    var partitionTable = sc.broadcast(Array[(Int, Int)]().toMap)
   
    val partitionedRDD = index.partitionBy(new SimilarityHashPartitioner(numPartitions, partitionTable))
    

    val indexed = partitionedRDD.mapPartitionsWithIndex((partitionId, iter) => {
      val data = iter.toArray
      val index = JaccardIndex(data, threshold, frequencyTable, multiGroup, minimum.value, alpha, numPartitions)
      Array(IPartition(partitionId, index, data
        .map(x => ((sortByValue(x._2._1._1).hashCode, x._2._1._2, 
          createInverse(sortByValue(x._2._1._1), multiGroup.value, threshold)
        .map(x => {
          if (x._1.length > 0) {
            (x._1.split(" ").map(s => s.hashCode), Array[Boolean]())
          } else {
            (Array[Int](), Array[Boolean]())
          }
        })), x._2._2)))).iterator
      }).persist(StorageLevel.MEMORY_AND_DISK_SER)
    indexed.count

    var indexRDD = indexed


  /* 

  FOR QUERY DATA RDD

  */
   val query_rdd = queryRDD
      .map(x => (sortByValue(x._1), x._2))
      // .distinct
      .map(x => ((x._1.hashCode, x._2, x._1),
        partition_r(
          x._1, frequencyTable, partitionTable, minimum.value, multiGroup,
          threshold, alpha, numPartitions, topDegree
        )))
      .flatMapValues(x => x)
      .map(x => {
       // println(s"#query_rdd_1 : (" + x._1._1 + " / " + x._1._2 + " / " + x._1._2 + ")")
        //for(e <- x._2._2) println(s"  >>qr1: x._2._2 : " + e._1 +","+ e._2+","+e._3.mkString(" ")+","+e._4+","+e._5)
        ((x._1._1, x._1._2, x._2._1), x._2._2)
      })
      .flatMapValues(x => x)
      .map(x => {
       // println(s"#query_rdd_2 : (" + x._2._1 + ") ,( " + x._1 + "/ "+  x._2._2 + ",/"+  x._2._3.mkString(" ")+ "/" + x._2._4 + "/" + x._2._2 + ")" )
        //for(e <- x._1._3) println(s"  >>qr2: x._1._3 : " +e._1.mkString(" ")+ " / "+ e._2.mkString(" "))
        (x._2._1, (x._1, x._2._2, x._2._3, x._2._4, x._2._5))
      })

    def Has(x : Int, array: Array[Int]): Boolean = {
      for (i <- array) {
        if (x == i) {
          return true
        }
      }
      false
    }
    val partitionLoad = query_rdd
      .mapPartitions({iter =>
        Array(distribute.clone()).iterator
      })
      .collect
      .reduce((a, b) => {
        val r = ArrayBuffer[Long]()
        for (i <- 0 until numPartitions) {
          r += (a(i) + b(i))
        }
        r.toArray.map(x => (x/numPartitions) * 8)
      })

    val maxPartitionId = sc.broadcast({
      val result = ArrayBuffer[Int]()
      for (l <- 0 until partitionNumToBeSent) {
        var max = 0.toLong
        var in = -1
        for (i <- 0 until numPartitions) {
          if (!Has(i, result.toArray) && partitionLoad(i) > max) {
            max = partitionLoad(i)
            in = i
          }
        }
        result += in
      }
      result.toArray
    })

    val extraIndex = sc.broadcast(
      indexRDD.mapPartitionsWithIndex((Index, iter) => {
        Array((Index, iter.toArray)).iterator
      }).filter(x => Has(x._1, maxPartitionId.value))
        .map(x => x._2)
        .collect())

    val query_rdd_partitioned = new SimilarityRDD(
      query_rdd.partitionBy(
          new SimilarityQueryPartitioner(
             numPartitions, partitionTable, frequencyTable, maxPartitionId.value)
        ), true
    ).persist(StorageLevel.MEMORY_AND_DISK_SER)
  

    /* INDEX x QUERY */

    println("\n===>  indexRDD")
    for(e <- indexRDD) {
    //  println(s"\n - partitonID : "+e._1 + "\n - Index : "+e._2 + "\n - raw_hashcode : "+ e._3(0)._1._1 + "\n  - raw data : "+e._3(0)._1._2 +"\n  - signature token(only string) hashcode : "+e._3(0)._1._3(0)._1.mkString(" ") +"\n  - signature token(only string) hashcode : "+e._3(0)._1._3(1)._1.mkString(" ") +"\n - bool : "+e._3(0)._2)
    }

    println("===>  query_rdd_partitioned")
    for(e <- query_rdd_partitioned){
     // println(s"\n[signature hashcode : "+e._1+"]" + "\n - IsDel : "+e._2._2 +"\n - IsTwo : "+e._2._3.mkString(" ") + "\n - IsExtend : "+e._2._4 + "\n - segNUm : "+e._2._5 +"\n - raw_hashcode : "+e._2._1._1 + "\n    - raw data : "+e._2._1._2 + "\n    - partition_r_records_code(signature token hashcode) : "+e._2._1._3(0)._1.mkString(" ") + "\n    - partition_r_records_bool : "+e._2._1._3(0)._2.mkString(" ") + "\n    - partition_r_records_code(signature token hashcode) : "+e._2._1._3(1)._1.mkString(" ") + "\n    - partition_r_records_bool : "+e._2._1._3(1)._2.mkString(" "))
    }

    // for saving answer, compareList 
    var ans = mutable.ListBuffer[(String, String)]()
    var comList= mutable.ListBuffer[(((Int, String, Array[(Array[Int], Array[Boolean])]), Boolean, Array[Boolean], Boolean, Int)  , ((Int, String, Array[(Array[Int], Array[Boolean])]), Boolean)) ]()
 

    val final_result = query_rdd_partitioned.zipPartitions(indexRDD, true) {
      (leftIter, rightIter) => {
        println(s"zipPartitions")
        val index = rightIter.next
        val index2 = extraIndex.value
        var countNum:Double = 0.0
        var pId = 0
        val partitionId = index.partitionId
        while (leftIter.hasNext) {

          val q = leftIter.next
          val positionOfQ = partitionTable.value.getOrElse(q._1, hashStrategy(q._1))
          val (candidate, whichIndex) = {
            if (positionOfQ != partitionId) {
              var (c, w) = (List[Int](), -1)
              var goon = true
              var i = 0
              while (goon && i < index2.length) {
                  if (index2(i)(0).partitionId == positionOfQ) {
                  c = index2(i)(0).index.asInstanceOf[JaccardIndex].index.getOrElse(q._1, List())
                  w = i
                  goon = false
                }
                i += 1
              }
              (c, w)
            } else {
              (index.index.asInstanceOf[JaccardIndex].index.getOrElse(q._1, List()), -1)
            }
          }
          //println(s"candidate : (" + candidate.mkString(" ") + ")")
          for (i <- candidate) {
            val data = {
              if (whichIndex < 0) {
                index.data(i)
              } else {
                index2(whichIndex)(0).data(i)
              }
            }
            if (compareSimilarity(q._2, data)) {
              //println(s"ans+=Tuple2 : (" + q._2._1._2 + ", " + data._1._2 + " )")
              ans += Tuple2(q._2._1._2, data._1._2)
            }
          }
        }
        //ans.map(x => new JoinedRow(x._2, x._1)).iterator
        
        ans.map(x => {
          //println(s"====> zipPartitions : (" + x._2 + " ///// " + x._1 + " )")
          (x._2, x._1)
        }).iterator
      }
    }.persist

    val result_out = final_result.distinct().count()//.distinct().collect()
    println("final_result.count(): "+result_out)
    println(s"===>  Finish")

    //sc.parallelize(result_out).saveAsTextFile("./final_result")
    }
  }

}
