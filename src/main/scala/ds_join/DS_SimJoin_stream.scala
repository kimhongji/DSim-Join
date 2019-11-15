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

./bin/spark-shell --packages org.mongodb.scala:mongo-scala-driver_2.11:2.0.0 org.mongodb.scala:mongo-scala-bson_2.11:2.0.0 org.mongodb:bson.0.0/home/user/Desktop/hongji/Dima_Ds_join/target/scala-2.11/Dima-DS-assembly-1.0.jar

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
      val stream = ssc.socketTextStream("192.168.0.15", 9999)
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

     // var queryRDD:org.apache.spark.rdd.RDD[(String, String)] = null
      var cacheTmp: org.apache.spark.rdd.RDD[(Int, ((String, String), Boolean))] = null    // for cache update

      var LRU_RDD: org.apache.spark.rdd.RDD[(Int, Int)] = null
      var LRU_Tmp: org.apache.spark.rdd.RDD[(Int, Int)] = null      // for LRUKey update

      var globalCacheCount: Long = 0

      var streaming_data_all: Int = 0
      var time_all = 0

      var cachedPRDD:org.apache.spark.rdd.RDD[(Int, ((String, String), Boolean))] = null
      //var index:org.apache.spark.rdd.RDD[(Int, ((String, String), Boolean))] = null

      var multiGroup:Broadcast[Array[(Int, Int)]] = null
      var frequencyTable: Broadcast[scala.collection.Map[(Int, Boolean), Long]] = null
      var partitionTable: Broadcast[scala.collection.immutable.Map[Int, Int]] = null


      

      var CacheThread: Thread = null
      var RemoveListThread: Thread = null
      var EndCondition: Thread = null
      var HitThread: Thread = null

      var missedIPRDDCount: Long = 0

      var isEmpty_missedData = false
      var DB_count:Long = 0
      var cachedPRDDDataCount:Long = 0
      var cachedDataCount:Long = 0
      var query_count:Long = 0
      var hitdimacount:Long = 0
      var inputKeysRDD_count:Long = 0

      var hit_sum:Long = 0
      var query_sum:Long = 0
      var cogroup_query_cache_sum:Long = 0
      var hit_dima_sum:Long = 0
      var inputKeysRDD_sum:Long = 0
      var LRU_sum:Long = 0
      var DB_get_sum:Long = 0
      var query_mapParition_sum:Long = 0
      var cwa_sum:Long = 0
      var miss_dima_sum:Long = 0
      var cached_sum:Long = 0
      var cache_time_sum:Long = 0
      var latency_sum:Long = 0
      var union_sum:Long = 0

      val data_num = args(0).toString
      //val db_coll_name = "Musical_Sig"+data_num
      val db_coll_name = "musical_sig"+data_num
      val coll_name = "mongodb://192.168.0.15:27017/REVIEW.musical_"+data_num 
      val cache_name = "/home/user/Desktop/hongji/ref/review_data/Musical_Instruments_sig1000.json"   
      var qlist = List[Int]()

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

      var shashP = new SimilarityHashPartitioner(partition_num, partitionTable)

      /*cache collection*/

      var cache_file = sqlContext.read.json(cache_name)
      var rows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = cache_file.rdd
      cachedPRDD = rows.map( x => (x(3).asInstanceOf[Long].intValue(),((x(0).toString,x(2).toString),x(1).toString.toBoolean)))
      cachedPRDD = cachedPRDD.partitionBy(shashP).cache()
            
      
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
      var start_total = System.currentTimeMillis
      stream.foreachRDD({ rdd => 
        if(!rdd.isEmpty()){

          val tStart = System.currentTimeMillis
          var compSign = 1
          var DB_PRDD:org.apache.spark.rdd.RDD[(Int, ((String, String), Boolean))] = null
          var queryIRDD:org.apache.spark.rdd.RDD[(Int, ((Int, String, Array[(Array[Int], Array[Boolean])]), Boolean, Array[Boolean], Boolean, Int))] = null
         // var joinedPRDD_missed:org.apache.spark.rdd.RDD[(Int, String)] = null 
         // var cogroupedRDD:org.apache.spark.rdd.RDD[(Int, (Iterable[((Int, String, Array[(Array[Int], Array[Boolean])]), Boolean, Array[Boolean], Boolean, Int)], Iterable[((String, String), Boolean)]))] = null
         // var missedRDD:org.apache.spark.rdd.RDD[(Int, ((Int, String, Array[(Array[Int], Array[Boolean])]), Boolean, Array[Boolean], Boolean, Int))] = null
          var missedIPRDD:org.apache.spark.rdd.RDD[(String, String)] = null 
          //var joinedPRDD_missed_total:(org.apache.spark.rdd.RDD[(Int, String)], org.apache.spark.SparkContext) = null
          var hit_dima_RDD:org.apache.spark.rdd.RDD[(Int, String)] = null
          var hitquery:org.apache.spark.rdd.RDD[(String, String)] = null
          var hitcache:org.apache.spark.rdd.RDD[(Int, ((String, String), Boolean))] = null

          var outputCount: Long = 0

          isEmpty_missedData = false

          println("\n\nStart|Stream num: " + streamingIteration)
       

          var input_file = sqlContext.read.json(rdd)
          var rows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = input_file.rdd
          var queryRDD = rows.map( x => (x(1).toString, x(1).toString)).filter(s => !s._1.isEmpty)//.partitionBy(hashP)
          val query_hashRDD = queryRDD.map(x => (x._1.hashCode(), x._1))
          query_count = queryRDD.count()

          println("data|qc|query_count : " + query_count)
          query_sum = query_sum + query_count

          var t0 = System.currentTimeMillis 
              

          var queryForIndex = queryRDD.map(x => (DimaJoin.sortByValue(x._1), x._2))
                  .map(x => ((x._1.hashCode, x._2, x._1),
                   DimaJoin.partition_r_all(
                       x._1, frequencyTable, partitionTable, minimum, multiGroup,
                         threshold, alpha, partition_num, topDegree
                     )))
                  .flatMapValues(x => x)
                  .map(x => { ((x._1._1, x._1._2, x._2._1), x._2._2)})
                  .flatMapValues(x => x)
                  .map(x => { (x._2._1, (x._1, x._2._2, x._2._3, x._2._4, x._2._5))}).cache() //x._2._1 => sig 


          println("cachedPRDD.partitioner: "+cachedPRDD.partitioner)    //Hash
          var cogroupedRDD = queryForIndex.cogroup(cachedPRDD).filter(s => (!s._2._1.isEmpty)).cache() // DATA FORMAT !!!!
          //println("cogroupedRDD debug"+cogroupedRDD.toDebugString )
          cogroupedRDD.count   
          //(query signature hascode, ( iter(query sig), iter(cache sig)))   
                
          var t1 = System.currentTimeMillis

          println("time|1|cogroup(query ,cache): " + (t1 - t0) + " ms")
          cogroup_query_cache_sum = cogroup_query_cache_sum + t1 - t0
          currCogTime = t1 - t0
          /* Thread */

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

                inputKeysRDD_count = inputKeysRDD.count()

                println("log|bc|inputKeysRDD count: "+inputKeysRDD_count)
                inputKeysRDD_sum = inputKeysRDD_sum + inputKeysRDD_count
               
                //println("inputKeysRDD.partitoner"+inputKeysRDD.partitioner) //hash
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
                LRU_sum = LRU_sum + t1 - t0
              }
          } //LRUKeyThread END



          RemoveListThread = new Thread(){
            override def run = {
              var t0 = System.currentTimeMillis
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

              var query_Count = query_count        // new query count
              var querysig_Count = inputKeysRDD_count // new query sig count
              var DB_Count = DB_count                 // after query 
              var cache_Count =  cachedDataCount      // before update
              var hit_Count = hitdimacount            // hit sig count (actually)
              var k = 1

              //start load balancing
              if(streamingIteration_th > 1000000){ // 5 is random value
                  if( hitdimacount > querysig_Count * 0.8 || hitdimacount < querysig_Count * 0.2 ) k = 2
                  else k = 1 

                  if( hitdimacount < querysig_Count * 0.4){
                      cachingWindow_th += k  

                  }else if( hitdimacount > querysig_Count * 0.5 ){
                      cachingWindow_th -= k    
                  }
                  // else : keep cachingWindow
                  

                  if(cachingWindow_th < 0){
                      cachingWindow_th = 1
                  }

              }else{
                cachingWindow_th = 10
                sCachingWindow = cachingWindow_th
              }
              //end load balancing

              println("data|cwa|caching window size: " + cachingWindow_th)
              cwa_sum = cwa_sum + cachingWindow_th

              var threshold_curr = streamingIteration_th - cachingWindow_th // n-c cache window size inc -> threshod dec -> 

              LRUKeyThread.join()
              removeList = LRU_RDD.filter({ s =>  s._2 < threshold_curr  })
              var removeList_count = removeList.count()
              println("data|rc|removeList_count: " + removeList_count)

              this.synchronized{
                cachingWindow = cachingWindow_th
              }
              
              var t1 = System.currentTimeMillis
              println("time|L|removeList Thread: " + (t1 - t0) + " ms")

              CacheThread.start           

            }
          } // RemoveListThread END



          CacheThread = new Thread(){
            override def run = {
              var enableCacheCleaningFunction_th = enableCacheCleaningFunction
              var streamingIteration_th = streamingIteration
              var isEmpty_missedData_th = isEmpty_missedData

              RemoveListThread.join()

              var t0 = System.currentTimeMillis

              if(enableCacheCleaningFunction_th == false){// disable cache cleaning
                  cacheTmp = cachedPRDD.union(DB_PRDD)
              }else{
                if(isEmpty_missedData_th){
                  cacheTmp = cachedPRDD

                }else if(!removeList.isEmpty){
                  cacheTmp = cachedPRDD.subtractByKey(removeList, shashP).union(DB_PRDD)
                  isPerformed_CC_PrevIter = true

                }else {
                  cacheTmp = cachedPRDD.union(DB_PRDD)

               }
              }
              if(streamingIteration % checkoutval == 0){
                println("=======Localchechpoint======")
                cacheTmp.localCheckpoint
              }
               
              cachedDataCount = cacheTmp.cache.count // check cache cout
              
              println("data|c|cached count(after union): " + cachedDataCount)  
              cached_sum = cached_sum + cachedDataCount
                    
              cachedPRDD.unpersist()
              cachedPRDD = cacheTmp
             
              var t1 = System.currentTimeMillis
              println("time|6|create cachedPRDD(currCacheTime): " + (t1 - t0) + " ms")
              cache_time_sum = cache_time_sum + currCacheTime           
              currCacheTime = t1 - t0

            }
          }// CacheThread END


          EndCondition = new Thread(){
            override def run = {
              if(streaming_data_all > 2000 )   ssc.stop()
            }
          }// EndCondition END

          HitThread = new Thread(){
            override def run = {
                var t2 = System.currentTimeMillis
                var hitedRDD = cogroupedRDD.filter(s => (!s._2._2.isEmpty))
                  .flatMapValues(pair => for(v <- pair._1.iterator; w <- pair._2.iterator) yield (v, w))
                  //.partitionBy(hashP) //add partitionby

                //hitedRDD.cache()

                hitcache= hitedRDD.mapValues(x => (x._2)).cache()  //cache index (signature)
                hitdimacount = hitcache.count()
                
                var t3 = System.currentTimeMillis
                //currCogTime = t3 - t2

                println("data|hc|hitdata dima(sig) : "+hitdimacount)
                println("time|3|hit dima time(currCogTime): " + (t3 - t2) + " ms")
                hit_sum = hit_sum + hitdimacount
                hit_dima_sum = hit_dima_sum + t3 - t2              
            }

          }
          /* miss data join thread */
          val missedFuture = Future{

              var missedRDDThread: Thread = null
              var missedRDD = cogroupedRDD.filter( s => (s._2._2.isEmpty))
                        .flatMapValues{case(x,y)=>x}

              missedRDD.cache()

             

              /* query DB */
              if(!isEmpty_missedData){
            
                  //missedRDDThread.join()

                  /*build query signature*/             

                  t0 = System.currentTimeMillis
                  
                  //println("missedRDD.partitioner: "+missedRDD.partitioner) //HashPartitioner
                  DB_PRDD = missedRDD.mapPartitions({ iter =>
                      var client: MongoClient = MongoClient("mongodb://192.168.0.10:27017") //mongos server
                      var database: MongoDatabase = client.getDatabase("musical")
                      //var database: MongoDatabase = client.getDatabase("musical")
                      var collection: MongoCollection[Document] = database.getCollection(db_coll_name) 
                     
                      var qlist_map = qlist
                      var dbData:Array[(Int, ((String, String), Boolean))] = Array() //xx // old version 
                      
                        if(!iter.isEmpty){

                          iter.foreach(q =>
                            qlist_map ::= q._1.toInt
                          )

                          var query = in("signature", qlist_map:_*) // the query filter
                          var temp = collection.find(query) //.map(x => (x.getInteger("signature").toInt,((x.getString("inverse"), x.getString("raw")), x.getBoolean("isDel").toString.toBoolean))) //for Document
                          //var temp = collection.distinct("signature").filter(query)

                          /*
                          temp.subscribe(new Observer[org.mongodb.scala.Document]){

                          }
                          */

                          var awaited = Await.result(temp.toFuture, scala.concurrent.duration.Duration.Inf)

                          for(data <- awaited){
                            dbData +:= (data.getInteger("signature").toInt,((data.getString("inverse"), data.getString("raw")), data.getBoolean("isDel").toString.toBoolean))  
                          }                  
                        }
                        client.close()
                        
                        var db_arr = dbData.map( s => (s._1, ((s._2._1._1, s._2._1._2), s._2._2)))
                        db_arr.iterator
                        
                   }, preservesPartitioning = true)
                   

                  DB_PRDD = DB_PRDD.cache()
                  DB_count = DB_PRDD.count()
                  println("[d]data|dc|DB get count: " + DB_count ) 
                  DB_get_sum = DB_get_sum + DB_count              

                  t1 = System.currentTimeMillis

                  println("time|4|query_mapPartition & cache_buildIndex data(currDBTime): " + (t1 - t0) + " ms")
                  currDBTime = t1 - t0 
                  query_mapParition_sum = query_mapParition_sum + currDBTime

                  HitThread.join

                  missedRDD.unpersist()

                  RemoveListThread.start
  
                  /* join missed data */
                  t0 = System.currentTimeMillis   
                  //println("DB_PRDD.partitioner: "+DB_PRDD.partitioner) //none ( distinct -> partitoner forget )
                  println("queryRDD.partitioner: "+queryRDD.partitioner) //hash
                  var sigUnion = hitcache.union(DB_PRDD)
                  var joinedPRDD_missed_total = DimaJoin.main(sc, sigUnion, queryRDD , frequencyTable, partitionTable, multiGroup, minimum, partition_num)
              

                  var joinedPRDD_missed = joinedPRDD_missed_total._1
                  sc = joinedPRDD_missed_total._2
                  
                  //joinedPRDD_missed.localCheckpoint
                  

                  outputCount = joinedPRDD_missed.count()
                  println("data|jm|joinedPRDD_missed_hit_count: " + outputCount) 
                  //println("data|out|output data: " + outputCount)          


                  t1 = System.currentTimeMillis
                  println("time|5|miss dimajoin (DB_PRDD, missedIPRDD) " + (t1 - t0) + " ms")
                  miss_dima_sum = miss_dima_sum + t1 - t0
                  
            
             } 
            else{
                println("time|4|create query + get data + create new RDD: 0 ms")
                currDBTime = 0
                RemoveListThread.start
                println("data|jm|joined_miss count: 0")
                println("time|5|join - miss data: 0 ms")

            } 

          union_sum = union_sum + 0

          CacheThread.join()

          } //missedFuture END


          /* ------- main ------*/

          if(enableCacheCleaningFunction){
            LRUKeyThread.start
          }

          EndCondition.start()

          HitThread.start()
          var ct0 = System.currentTimeMillis
          val n =Await.result(missedFuture, scala.concurrent.duration.Duration.Inf)
          var ct1 = System.currentTimeMillis
          println("time|fu|missedFuture time: " + (ct1 - ct0) + " ms") 
            
          
          cogroupedRDD.unpersist()
          rdd.unpersist()
          hitcache.unpersist()
          DB_PRDD.unpersist()
          queryForIndex.unpersist()
          
          //hitedRDD.unpersist()


          val tEnd = System.currentTimeMillis
          currStreamTime = tEnd - tStart
          println("time|8|latency: " + currStreamTime + " ms")
          latency_sum = latency_sum + currStreamTime

          streamingIteration = streamingIteration + 1

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


         // if(!isEmpty_missedData){
          //  
          //}
        }
      })

      ssc.start()
      ssc.awaitTermination()
      var end_total = System.currentTimeMillis
      var total_time = end_total - start_total

      println("\n\n======Streaming average log=====\n")
      println("> total streaming iteration : "+streamingIteration)
      println("data|query_sum: " + query_sum/streamingIteration)
      println("time|cogroup_query_cache_sum: "+cogroup_query_cache_sum/streamingIteration+" ms")   
      println("data|hit sum: " + hit_sum/streamingIteration)
      println("time|hit_dima_sum: "+hit_dima_sum/streamingIteration+" ms")
      println("data|inputKeysRDD_sum: " + inputKeysRDD_sum/streamingIteration)
      println("time|LRU_sum: "+LRU_sum/streamingIteration+" ms")
      println("data|DB_get_sum: " + DB_get_sum/streamingIteration)
      println("time|query_mapParition_sum: "+query_mapParition_sum/streamingIteration+" ms")
      println("data|cwa_sum: " + cwa_sum/streamingIteration)
      println("time|miss_dima_sum: "+miss_dima_sum/streamingIteration+" ms")
      println("data|cached_sum: " + cached_sum/streamingIteration)
      println("time|cache_time_sum: "+cache_time_sum/streamingIteration+" ms")
      println("time|union_sum: "+union_sum/streamingIteration+" ms")
      println("data|streaming data all: " + streaming_data_all)
      println("time|latency_sum: "+latency_sum/streamingIteration+" ms")
      println("time|total time: "+total_time+" ms")
      println("\n=================================\n")

      



    }


 }