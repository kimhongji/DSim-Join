package ds_join

import com.mongodb.spark._
import com.mongodb.spark.config._
import com.mongodb.spark.sql._
import org.mongodb.scala._
import org.mongodb.scala.Document._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.bson._

//import org.bson.Document

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner
import org.apache.spark.RangePartitioner
import org.apache.spark.storage.StorageLevel
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.collection.mutable


/*Complile*/
/*
1. for shell : ./bin/spark-shell --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/REVIEPreferred"\
--packages org.mongodb.spark:mongo-spark-connector_2.11:2.1.0 /home/user/Desktop/hongji/Dima_Ds_join/target/scala-2.11/Dima-DS-assembly-1.0.jar

./bin/spark-shell --packages org.mongodbcala:mongo-scala-driver_2.11:2.0.0 org.mongodb.scala:mongo-scala-bson_2.11:2.0.0 org.mongodb:bson.0.0/home/user/Desktop/hongji/Dima_Ds_join/target/scala-2.11/Dima-DS-assembly-1.0.jar

2. for submit : ./bin/spark-submit --class ds_join.DS_SimJoin_stream --master $master /home/user/Desktop/hongji/Dima_Ds_join/target/scala-2.11/Dima-DS-assembly-1.0.jar $num 

*/


/*check it is partitioner right?*/
object DS_SimJoin_stream{

  def main(args: Array[String]){
      
      /*Initialize variable*/
      var conf = new SparkConf().setAppName("DS_SimJoin_stream")
      var sc = new SparkContext(conf)
      var sqlContext = new SQLContext(sc)
      val ssc = new StreamingContext(sc, Milliseconds(3000)) // 700
      val stream = ssc.socketTextStream("192.168.0.11", 9999)
      var AvgStream:Array[Long] = Array()

      var partition_num:Int = 4
      val threshold:Double = 0.8  // threshold!!!!!!!
      val alpha = 0.95
      var minimum:Int = 0
      var topDegree = 0
      var hashP = new HashPartitioner(partition_num)
      var streamingIteration = 1

      var cachingWindow = 1
      var pCachingWindow = 1
      var ppCachingWindow = 1
      var pppCachingWindow = 1
      var sCachingWindow = 1
      var sCachingWindow_pre = 1
      var sCachingWindow_preTime: Long = 0
      var sCachingWindow_time: Long = 0
      var alphaValue: Long = 215
      val checkoutval = 10 //

      var enableCacheCleaningFunction = true
      var isPerformed_CC_PrevIter = false

      var delCacheTimeList: List[Int] = null
      var removeList: org.apache.spark.rdd.RDD[(Int, Int)] = null

      var currCogTime: Long = 0
      var currCacheTime: Long = 0
      var currDBTime: Long = 0
      var currStreamTime: Long = 0

      var pCogTime: Long = 0
      var ppCogTime: Long = 0
      var pppCogTime: Long = 0

      var pCacheTime: Long = 0
      var ppCacheTime: Long = 0
      var pppCacheTime: Long = 0
      var ppppCacheTime: Long = 0

      var pDBTime: Long = 0
      var ppDBTime: Long = 0
      var pppDBTime: Long = 0

      var pIterationTime: Long = 0
      var ppIterationTime:Long = 0
      var pOutputCount: Long = 0

      var missedKeysCount: Long = 0
      var pMissedKeysCount: Long = 0
      var ppMissedKeysCount: Long = 0

      var queryRDD:org.apache.spark.rdd.RDD[(String, String)] = null
      //var cachedPRDD: org.apache.spark.rdd.RDD[(Int, String)] = null
      var cacheTmp: org.apache.spark.rdd.RDD[(Int, ((String, String), Boolean))] = null    // for cache update

      var LRU_RDD: org.apache.spark.rdd.RDD[(Int, Int)] = null
      var LRU_Tmp: org.apache.spark.rdd.RDD[(Int, Int)] = null      // for LRUKey update

      var globalCacheCount: Long = 0

      var streaming_data_all: Int = 0
      var time_all = 0

      //var buildIndexSig:org.apache.spark.rdd.RDD[(Int,((String, String) ,Boolean))] = null
      var sig_ppreRDD:org.apache.spark.rdd.RDD[org.bson.Document] = null
      var cachedPRDD:org.apache.spark.rdd.RDD[(Int, ((String, String), Boolean))] = null
      var index:org.apache.spark.rdd.RDD[(Int, ((String, String), Boolean))] = null
      //Origin//var cogroupedRDD:org.apache.spark.rdd.RDD[(Int, (Iterable[String], Iterable[String]))] = null
      //var cogroupedRDD:org.apache.spark.rdd.RDD[(Int, (Iterable[((String, String), Boolean)], Iterable[((String, String), Boolean)]))] = null
      var cogroupedRDD:org.apache.spark.rdd.RDD[(Int, (Iterable[((Int, String, Array[(Array[Int], Array[Boolean])]), Boolean, Array[Boolean], Boolean, Int)], Iterable[((String, String), Boolean)]))] = null
      //Origin//var hitedRDD:org.apache.spark.rdd.RDD[(Int, (String, String))] = null
      var hitedRDD:org.apache.spark.rdd.RDD[(Int, (((Int, String, Array[(Array[Int], Array[Boolean])]), Boolean, Array[Boolean], Boolean, Int), ((String, String), Boolean)))] = null
      var missedRDD:org.apache.spark.rdd.RDD[(Int, (Iterable[String], Iterable[String]))] = null
      //var missedRDD:org.apache.spark.rdd.RDD[(Int, (Iterable[((String, String), Boolean)], Iterable[((String, String), Boolean)]))] = null
      var missedFRDD:org.apache.spark.rdd.RDD[(Int, String)] = null
      //var missedFRDD:org.apache.spark.rdd.RDD[(Int, ((String, String), Boolean))] = null
      var missedIPRDD:org.apache.spark.rdd.RDD[(String, String)] = null
      //var missedIPRDD:org.apache.spark.rdd.RDD[(((Int, String, Array[(Array[Int], Array[Boolean])]), Boolean, Array[Boolean], Boolean, Int), ((Int, String, Array[(Array[Int], Array[Boolean])]), Boolean, Array[Boolean], Boolean, Int))] = null
      var partitionedRDD:org.apache.spark.rdd.RDD[(Int, ((String, String), Boolean))] = null
      var indexed:org.apache.spark.rdd.RDD[IPartition] = null
      var cachedIRDD:org.apache.spark.rdd.RDD[IPartition] = null
      var queryIRDD:org.apache.spark.rdd.RDD[(Int, ((Int, String, Array[(Array[Int], Array[Boolean])]), Boolean, Array[Boolean], Boolean, Int))] = null
      var queryForIndex:org.apache.spark.rdd.RDD[(Int, ((Int, String, Array[(Array[Int], Array[Boolean])]), Boolean, Array[Boolean], Boolean, Int))] = null
      var DB_IRDD:org.apache.spark.rdd.RDD[IPartition] = null
      var multiGroup:Broadcast[Array[(Int, Int)]] = null

      var frequencyTable: Broadcast[scala.collection.Map[(Int, Boolean), Long]] = null
      var partitionTable: Broadcast[scala.collection.immutable.Map[Int, Int]] = null
      

      var db_sig_ppreRDD:org.apache.spark.rdd.RDD[org.bson.Document] = null
      var db_sig_preRDD:org.apache.spark.rdd.RDD[(Int, ((String, String), Boolean))] = null
      var DB_PRDD:org.apache.spark.rdd.RDD[(Int, ((String, String), Boolean))] = null
      var DB_PRDD_filter:org.apache.spark.rdd.RDD[(Int, ((String, String), Boolean))] = null
      var joinedPRDD_missed_total:(org.apache.spark.rdd.RDD[(Int, String)], org.apache.spark.SparkContext) = null
      var joinedPRDD_missed:org.apache.spark.rdd.RDD[(Int, String)] = null      
      var hit_dima_PRDD:(org.apache.spark.rdd.RDD[(Int, String)], org.apache.spark.SparkContext) = null
      var hit_dima_RDD:org.apache.spark.rdd.RDD[(Int, String)] = null

      var CacheThread: Thread = null
      var RemoveListThread: Thread = null
      var EndCondition: Thread = null

      var missedIPRDDCount: Long = 0

      var isEmpty_missedData = false
      var DB_count:Long = 0
      var cachedPRDDDataCount:Long = 0
      var cachedDataCount:Long = 0
      var query_count:Long = 0
      var hit_sum:Long = 0


      val data_num = args(0).toString
      val db_coll_name = "musical_sig"+data_num
      val coll_name = "mongodb://192.168.0.11:27017/REVIEW.musical_"+data_num 
      val cache_name = "../ref/review_data/Musical_Instruments_sig100.json"   

      //change mongospark version = 2.2.6  to 2.1.0
      /*index collection*/
      val readConfig = ReadConfig(Map(
        "spark.mongodb.input.uri" -> coll_name,
        "spark.mongodb.input.readPreference.name" -> "primaryPreferred"      
       ))
      val load = MongoSpark.load(sc,readConfig)
      val preRDD = load.map( x => x.getString("reviewText"))
      val dataRDD = preRDD.map(x => (x,x))

      /*
       --  Run DIMA BuildIndex 
          -- set indexedRDD from buildIndex and f , multigroup        
      */
      var buildIndexSig = BuildSig.main(sc, dataRDD, partition_num) // buildIndexSig = tuple4 ( index, f , multiGroup, sc )
      frequencyTable = sc.broadcast(buildIndexSig._2.collectAsMap())
      multiGroup = buildIndexSig._3
      sc = buildIndexSig._4
      minimum = buildIndexSig._5
      partitionTable = sc.broadcast(Array[(Int, Int)]().toMap)

      /*cache collection*/

      var cache_file = sqlContext.read.json(cache_name)
      var rows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = cache_file.rdd
      cachedPRDD = rows.map( x => (x(3).asInstanceOf[Long].intValue(),((x(0).toString,x(2).toString),x(1).toString.toBoolean)))
      cachedPRDD = cachedPRDD.partitionBy(hashP).cache()

      //println(s"\n\n\n===> cachedPRDD")
      //cachedPRDD.collect().foreach(println)      // randmom                
      
      /* build LRU_RDD using index(cache) data */
      if(enableCacheCleaningFunction){
         LRU_RDD = cachedPRDD.map( x => (x._1, 0)).partitionBy(hashP) /*   !!!!!!!!!!!!!!!here hashc???!!!!!!!!!!!!!!!!!  */
         LRU_RDD.cache().count()
      }

      println("index coll: "+coll_name)
      println("cache coll: "+cache_name)
      println("sig_index coll: "+db_coll_name)
    
      /* Run DIMA Similarity Join 
      ===========================stream====================================
      */

      stream.foreachRDD({ rdd => 
        if(!rdd.isEmpty()){

          val tStart = System.currentTimeMillis
          var compSign = 1
          isEmpty_missedData = false

          println("\n\nStart|Stream num: " + streamingIteration)
       

          var input_file = sqlContext.read.json(rdd)
          var rows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = input_file.rdd
          queryRDD = rows.map( x => (x(1).toString, x(1).toString)).filter(s => !s._1.isEmpty)
          val query_hashRDD = queryRDD.map(x => (x._1.hashCode(), x._1))
          query_count = queryRDD.count()

          println("data|qc|query_count : " + query_count)

          var t0 = System.currentTimeMillis
          var t0_1 = System.currentTimeMillis

/*
          val inverseRDD = queryRDD
            .map(x => {(DimaJoin.sortByValue(x._1.toString),x._2)})

          val splittedRecord = inverseRDD
            .map(x => {
              ((x._1, x._2), DimaJoin.createInverse(x._1, multiGroup.value , threshold))
            })
            .flatMapValues(x => x)
           .map(x => {
              ((x._1, x._2._2, x._2._3), x._2._1)
            })

          val deletionIndexSig = splittedRecord
            .filter(x => (x._2.length > 0))
           .map(x => (x._1, DimaJoin.createDeletion(x._2))) // (1,i,l), deletionSubstring
            .flatMapValues(x => x)
            .map(x => {
              ((x._2, x._1._2, x._1._3).hashCode(), (x._1._1, true))
            })
          // (hashCode, (String, internalrow))

          val segIndexSig = splittedRecord
            .map(x => {
               ((x._2, x._1._2, x._1._3).hashCode(), (x._1._1, false))
            })

          val index = deletionIndexSig.union(segIndexSig)*/

          queryForIndex = queryRDD.map(x => (DimaJoin.sortByValue(x._1), x._2))
                   .map(x => ((x._1.hashCode, x._2, x._1),
                   DimaJoin.partition_r(
                      x._1, frequencyTable, partitionTable, minimum, multiGroup,
                      threshold, alpha, partition_num, topDegree
                    )))
                .flatMapValues(x => x)
                .map(x => { ((x._1._1, x._1._2, x._2._1), x._2._2)})
                .flatMapValues(x => x)
                .map(x => { (x._2._1, (x._1, x._2._2, x._2._3, x._2._4, x._2._5))}).cache() //x._2._1 => sig  

          cogroupedRDD = queryForIndex.cogroup(cachedPRDD).filter(s => (!s._2._1.isEmpty)).cache() // DATA FORMAT !!!!
          //cogroupedRDD = query_hashRDD.cogroup(dima_RDD).filter(s => (!s._2._1.isEmpty)).cache() // DATA FORMAT !!!!
                 
          
        var t1 = System.currentTimeMillis

          println("time|1|first dimajoin & cogroup (query-cached)(currCogTime1): " + (t1 - t0) + " ms")
          //currCogTime = t1 - t0

          /* hit data join thread */
         // val hitThread = new Thread(){
         //   override def run = {
             t0 = System.currentTimeMillis

              hitedRDD = cogroupedRDD.filter(s => (!s._2._2.isEmpty))
                .flatMapValues(pair => for(v <- pair._1.iterator; w <- pair._2.iterator) yield (v, w))
              hitedRDD.cache()

              val hitquery = hitedRDD.map(x => (x._2._1._1._2, x._2._1._1._2)).distinct().cache()      //query index (signature) ( string, string )
              val hitcache= hitedRDD.map(x => (x._1, x._2._2)).distinct().cache()  //cache index (signature)

              partitionedRDD = hitcache.partitionBy(new SimilarityHashPartitioner(partition_num, partitionTable))

              cachedIRDD = partitionedRDD.mapPartitionsWithIndex((partitionId, iter) => {
               val data = iter.toArray
                val index = JaccardIndex(data, threshold, frequencyTable, multiGroup, minimum, alpha, partition_num)
                Array(IPartition(partitionId, index, data
                  .map(x => ((BuildSig.sortByValue(x._2._1._1).hashCode, x._2._1._2, 
                    BuildSig.createInverse(BuildSig.sortByValue(x._2._1._1), multiGroup.value, threshold)
                  .map(x => {
                    if (x._1.length > 0) {
                      (x._1.split(" ").map(s => s.hashCode), Array[Boolean]())
                    } else {
                      (Array[Int](), Array[Boolean]())
                    }
                  })), x._2._2)))).iterator
                }, preservesPartitioning = true).cache()    // add preservePartitioning 

              hit_dima_PRDD = DimaJoin.main(sc, cachedIRDD, hitquery , frequencyTable, partitionTable, multiGroup, minimum, partition_num)
          
              hit_dima_RDD = hit_dima_PRDD._1.partitionBy(hashP)
              sc = hit_dima_PRDD._2 

              hitquery.unpersist()
              hitcache.unpersist()      

              val hitcount = hitedRDD.count()
              val hitdimacount = hit_dima_RDD.count()

              //println(s"\n\n\n===> hitedRDD")
              //hitedRDD.collect().foreach(println)// randmom

              t1 = System.currentTimeMillis
             var t1_1 = System.currentTimeMillis
              hit_sum = hit_sum + hitdimacount
              println("data|hc|hitdata count(sig): " + hitcount + ", hit dima(string) : "+hitdimacount)
              println("time|3|hitThread data(currCogTime2): " + (t1 - t0) + " ms")
              currCogTime = t1_1 - t0_1
          //  }
        //  }

          val hitThread = new Thread(){
            override def run = {

              
            }
          }
          /* miss data join thread */
          val missedFuture = Future{

              var t0 = System.currentTimeMillis
              var missedRDDThread: Thread = null

              var miss_cogroupedRDD = query_hashRDD.cogroup(hit_dima_RDD).filter(s => (!s._2._1.isEmpty)).cache() // DATA FORMAT !!!!
              missedRDD = miss_cogroupedRDD.filter(s => (s._2._2.isEmpty))
              missedRDD.cache()

              missedRDDThread = new Thread(){
                  override def run = {
                      missedFRDD = missedRDD.flatMapValues{case(x,y) => x}
                      missedIPRDD = missedFRDD.map(x => (x._2, x._2)).distinct().cache()

                      //missedIPRDD = missedFRDD.map(x => (x._2._1._2, x._2._1._2)).distinct().cache()

                      //println(s"\n\n\n===> missedFRDD")
                      //missedFRDD.collect().foreach(println)  // randmom

                      missedKeysCount = missedFRDD.cache().count()
                      println("data|mc|missedKeys count: " + missedKeysCount)
                  }
              }

              if(missedRDD.isEmpty){
                  isEmpty_missedData = true
                  println("data|mc|missedKeys count: 0")
              }else{
                  missedRDDThread.start()
              }

              /* query DB */
              if(!isEmpty_missedData){
            
                  missedRDDThread.join()

                  t0 = System.currentTimeMillis 

                  /*build query signature*/
                  queryIRDD = missedIPRDD
                     .map(x => (DimaJoin.sortByValue(x._1), x._2))
                     .map(x => ((x._1.hashCode, x._2, x._1),
                     DimaJoin.partition_r(
                        x._1, frequencyTable, partitionTable, minimum, multiGroup,
                        threshold, alpha, partition_num, topDegree
                      )))
                    .flatMapValues(x => x)
                    .map(x => { ((x._1._1, x._1._2, x._2._1), x._2._2)})
                    .flatMapValues(x => x)
                    .map(x => { (x._2._1, (x._1, x._2._2, x._2._3, x._2._4, x._2._5))}).cache()
              
                  DB_PRDD = queryIRDD.mapPartitions({ iter =>
                      var client: MongoClient = MongoClient("mongodb://192.168.0.11:27017")
                      var database: MongoDatabase = client.getDatabase("REVIEW")
                      var collection: MongoCollection[Document] = database.getCollection(db_coll_name) 
                      
                      var qlist = List[Int]()
                      var dbData:Array[(Int, ((String, String), Boolean))] = Array() //xx // old version 
                      
                        if(!iter.isEmpty){

                          iter.foreach(q =>
                            qlist ::= q._1.toInt
                          )
                          var query = in("signature", qlist:_*)
                          var temp = collection.find(query) //.map(x => (x.getInteger("signature").toInt,((x.getString("inverse"), x.getString("raw")), x.getBoolean("isDel").toString.toBoolean))) //for Document
                          
                          var awaited = Await.result(temp.toFuture, scala.concurrent.duration.Duration.Inf)

                          for(data <- awaited){
                            dbData +:= (data.getInteger("signature").toInt,((data.getString("inverse"), data.getString("raw")), data.getBoolean("isDel").toString.toBoolean))  
                          }                         
                        }
                        client.close()
                        
                        var db_arr = dbData.map( s => (s._1, ((s._2._1._1, s._2._1._2), s._2._2)))
                        db_arr.iterator
                        
                   }, preservesPartitioning = true)

                  DB_PRDD = DB_PRDD.distinct().cache()
                  DB_count = DB_PRDD.count()
                  println("data|dc|DB get count: " + DB_count )

                  t1 = System.currentTimeMillis

                  println("time|4|query_mapPartition & cache_buildIndex data(currDBTime): " + (t1 - t0) + " ms")
                  currDBTime = t1 - t0

                  //println(s"**===> DB_PRDD")
                  //DB_PRDD.collect().foreach(println)                  

                  val partitionedDB_RDD = DB_PRDD.partitionBy(new SimilarityHashPartitioner(partition_num, partitionTable))

                  DB_IRDD = partitionedDB_RDD.mapPartitionsWithIndex((partitionId, iter) => {
                    val data = iter.toArray
                    val index = JaccardIndex(data, threshold, frequencyTable, multiGroup, minimum, alpha, partition_num)
                    Array(IPartition(partitionId, index, data
                      .map(x => ((BuildSig.sortByValue(x._2._1._1).hashCode, x._2._1._2, 
                        BuildSig.createInverse(BuildSig.sortByValue(x._2._1._1), multiGroup.value, threshold)
                      .map(x => {
                       if (x._1.length > 0) {
                          (x._1.split(" ").map(s => s.hashCode), Array[Boolean]())
                        } else {
                          (Array[Int](), Array[Boolean]())
                        }
                      })), x._2._2)))).iterator
                    }, preservesPartitioning = true).cache()                  

 

                  RemoveListThread.start

                  /* join missed data */
                  t0 = System.currentTimeMillis                  

                  joinedPRDD_missed_total = DimaJoin.main(sc, DB_IRDD, missedIPRDD , frequencyTable, partitionTable, multiGroup, minimum, partition_num)
                  
                  joinedPRDD_missed = joinedPRDD_missed_total._1.partitionBy(hashP).cache()
                  sc = joinedPRDD_missed_total._2

                  var joinedPRDD_missed_count = joinedPRDD_missed.count()
                  println("data|jm|joined_miss count: " + joinedPRDD_missed_count)

                  //println(s"\n\n\n===> joinedPRDD_missed")
                  //joinedPRDD_missed.collect().foreach(println)      // randmom              

                  missedIPRDD.unpersist()
                  queryIRDD.unpersist()

                  t1 = System.currentTimeMillis
                  println("time|5|second dimajoin (DB_IRDD, missedIPRDD) " + (t1 - t0) + " ms")
                  
            
             } 
            else{
                println("time|4|create query + get data + create new RDD: 0 ms")
                currDBTime = 0
                RemoveListThread.start
                println("data|jm|joined_miss count: 0")
                println("time|5|join - miss data: 0 ms")

            }       
          } //missedFuture END

          var LRUKeyThread = new Thread(){
              override def run = {
                var t0 = System.currentTimeMillis

                var streamingIteration_th = streamingIteration
                var cachingWindow_th = cachingWindow
                var threshold_prev = streamingIteration_th - cachingWindow_th -1

                var inputKeysRDD = queryForIndex.mapPartitions({ iter => 
                  var newPartition = iter.map(s => (s._1, streamingIteration_th))
                  newPartition
                  }, preservesPartitioning = true)

                var inputKeysRDD_count = inputKeysRDD.count()

                println("log|bc|inputKeysRDD count: "+inputKeysRDD_count)
               
                if(isPerformed_CC_PrevIter){
                  LRU_Tmp = LRU_RDD.filter(s => s._2 >= threshold_prev)
                                   .subtractByKey(inputKeysRDD, hashP) // update and insert new cache data
                                   .union(inputKeysRDD)

                  isPerformed_CC_PrevIter = false
                }else{
                  LRU_Tmp = LRU_RDD.subtractByKey(inputKeysRDD, hashP)
                                   .union(inputKeysRDD)
                }

                if(streamingIteration_th % checkoutval == 0){
                  LRU_Tmp.localCheckpoint
                }

                LRU_Tmp.cache().count()
                LRU_RDD.unpersist()
                

                LRU_RDD = LRU_Tmp

                var t1 = System.currentTimeMillis
                println("time|9|LRU keys update time: " + (t1 - t0) + " ms")
              }
          } //LRUKeyThread END



          RemoveListThread = new Thread(){
            override def run = {
              t0 = System.currentTimeMillis

              var delCacheTimeList_th = delCacheTimeList 
              var enableCacheCleaningFunction_th = enableCacheCleaningFunction
              var streamingIteration_th = streamingIteration            

              var cachingWindow_th = cachingWindow
              var currCogTime_th = currCogTime
              var currDBTime_th = currDBTime

              var pCacheRelatedOpTimeDiff = currCogTime_th - pCogTime + pCacheTime - ppCacheTime
              var pDBTimeDiff = currDBTime_th - pDBTime

              println("data|cwb|caching window size: " + cachingWindow_th)

              var pAll = currCogTime_th + currDBTime_th + pCacheTime
              var ppAll = pCogTime + pDBTime + ppCacheTime
              var pppAll = ppCogTime + ppDBTime + pppCacheTime
              var isEmpty_missedData_th = isEmpty_missedData

              //start load balancing
              if(isEmpty_missedData_th){
                cachingWindow_th += 1
                sCachingWindow = cachingWindow_th
              }else if(streamingIteration_th > 3){ // 5 is random value 
                 if(pAll > ppAll){
                     cachingWindow_th = sCachingWindow

                 }else if(pAll < ppAll){
                     sCachingWindow = cachingWindow_th

                     if(currDBTime > currCogTime + pCacheTime ){
                        cachingWindow_th += 1   

                     }else if(currDBTime < currCogTime + pCacheTime && cachingWindow_th > 1){
                        cachingWindow_th -= 1     
                     }             
                }
              }else{
                cachingWindow_th += 1
                sCachingWindow = cachingWindow_th
              }

              println("data|cwa|caching window size: " + cachingWindow_th)

              var threshold_curr = streamingIteration_th - cachingWindow_th // n-c cache window size inc -> threshod dec -> 

              LRUKeyThread.join()
              removeList = LRU_RDD.filter({ s =>  s._2 < threshold_curr  })
              var removeList_count = removeList.count()
              println("data|rc|removeList_count: " + removeList_count)

              this.synchronized{
                cachingWindow = cachingWindow_th
              }
              
              CacheThread.start           

            }
          } // RemoveListThread END



          CacheThread = new Thread(){
            override def run = {
              var enableCacheCleaningFunction_th = enableCacheCleaningFunction
              var streamingIteration_th = streamingIteration
              var isEmpty_missedData_th = isEmpty_missedData

              var t0 = System.currentTimeMillis

              cachedPRDDDataCount = cachedPRDD.cache().count() // check cache cout
              println("data|c|cachedPRDDDataCount(before union) 1: " + cachedPRDDDataCount) 
              if(cachedPRDDDataCount > DB_count * 0.7 && DB_count < query_count){
                println("log|cu|cachedPRDD doesn't update")
                cacheTmp = DB_PRDD
              }else{              

                println("log|cu|cachedPRDD update")

                if(enableCacheCleaningFunction_th == false){// disable cache cleaning
                  cacheTmp = cachedPRDD.union(DB_PRDD).partitionBy(hashP)
                }else{

                 if(isEmpty_missedData_th){
                    cacheTmp = cachedPRDD
                  }else if(!removeList.isEmpty){
                    cacheTmp = cachedPRDD.subtractByKey(removeList, hashP).union(DB_PRDD)
                    //cacheTmp = cachedPRDD.subtractByKey(removeList, hashP).union(DB_PRDD_filter)
                    isPerformed_CC_PrevIter = true
                  }else {
                    cacheTmp = cachedPRDD.union(DB_PRDD).partitionBy(hashP)
                 }
                }
                if(streamingIteration % checkoutval == 0){
                  println("=======Localchechpoint======")
                  cacheTmp.localCheckpoint
                }
                cacheTmp = cacheTmp.distinct()
              }
               
              cachedDataCount = cacheTmp.cache().count() // check cache cout
              globalCacheCount = cachedDataCount
              println("data|c|cachedData(after union) 2: " + cachedDataCount)  
                    
              cachedPRDD.unpersist()
              cachedPRDD = cacheTmp
             
              var t1 = System.currentTimeMillis
              println("time|6|create cachedPRDD(currCacheTime): " + (t1 - t0) + " ms")
            
              currCacheTime = t1 - t0

            }
          }// CacheThread END


          EndCondition = new Thread(){
            override def run = {
              if(streamingIteration > 1000 )   ssc.stop()
            }
          }// EndCondition END


          /* ------- main ------*/

          if(enableCacheCleaningFunction){
            LRUKeyThread.start
          }

          EndCondition.start()

          //hitThread.start()
          t0 = System.currentTimeMillis
          val n =Await.result(missedFuture, scala.concurrent.duration.Duration.Inf)
          t1 = System.currentTimeMillis
          //hitThread.join()

          var outputCount: Long = 0

          /* union (hitedRDD, joinedRDD_missed) */
          t0 = System.currentTimeMillis
          if(isEmpty_missedData){
            outputCount = hitedRDD.count //origin hitedRDD.count
          }else{
            //var righthitedRDD = hitedRDD.map(x =>  (x._1, x._2._2)).partitionBy(hashP)
            var righthitedRDD = hit_dima_RDD
            
            var output = righthitedRDD.union(joinedPRDD_missed)

            val outputReduce = output
            outputCount = outputReduce.count()

           // println(s"\n\n\n===> outputReduce")
           // outputReduce.collect().foreach(x => print("("+x._1+" ,"+x._2.hashCode()+" ) ."))   //randmom 
          }
          println("data|out|output data: " + outputCount)       
          t1 = System.currentTimeMillis
          println("time|7|union output data: " + (t1 - t0) + " ms") 


          CacheThread.join()

          streamingIteration = streamingIteration + 1

          cogroupedRDD.unpersist()
          missedRDD.unpersist()
          rdd.unpersist()
          DB_PRDD.unpersist()
          //DB_PRDD_filter.unpersist()

          val tEnd = System.currentTimeMillis
          currStreamTime = tEnd - tStart
          println("time|8|stream: " + currStreamTime + " ms")
          AvgStream +:= currStreamTime
          var sum:Long = 0
          for(i <- AvgStream){
            sum = sum + i
          }

          var as = sum/AvgStream.length
          println("data|hs|hit sum: " + hit_sum)
          println("data|as|average stream: " + as + " ms "+AvgStream.length)


          pppCogTime = ppCogTime
          ppCogTime = pCogTime
          pCogTime = currCogTime

          pppDBTime = ppDBTime
          ppDBTime = pDBTime
          pDBTime = currDBTime

          ppppCacheTime = pppCacheTime        
          pppCacheTime = ppCacheTime
          ppCacheTime = pCacheTime
        
          ppIterationTime = pIterationTime
        
          ppMissedKeysCount = pMissedKeysCount

          ppCachingWindow = pCachingWindow
          pCachingWindow = cachingWindow
        
          pCacheTime = currCacheTime
        
          pOutputCount = outputCount
          pIterationTime = currStreamTime
          pMissedKeysCount = missedKeysCount
        
          streaming_data_all = streaming_data_all + outputCount.toInt
          println("data|all|streaming data all: " + streaming_data_all)

          queryForIndex.unpersist()
          hitedRDD.unpersist()
          if(!isEmpty_missedData){
            joinedPRDD_missed.unpersist()
          }
        }//if(rdd.isempty) end
      })

      ssc.start()
      ssc.awaitTermination()
    }
 }