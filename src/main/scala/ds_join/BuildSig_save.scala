package ds_join

import com.mongodb.spark._
import com.mongodb.spark.config._
import com.mongodb.spark.sql._
import org.mongodb.scala._
import org.mongodb.scala.Document._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Filters._
import org.bson.Document

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

import org.json4s.native.JsonMethods._
import org.json4s.JsonDSL.WithDouble._
import org.json4s.jackson.JsonMethods._
import java.io._
// ./bin/spark-shell --packages org.mongodb.scala:mongo-scala-driver_2.11:2.0.0 org.mongodb.scala:mongo-scala-bson_2.11:2.0.0 org.mongodb:bson.0.0/home/user/Desktop/hongji/Dima_Ds_join/target/scala-2.11/Dima-DS-assembly-1.0.jar

object BuildSig_save{

  def main(args: Array[String]){


      val conf = new SparkConf().setAppName("BuildSig_save")
      val sc = new SparkContext(conf)

      val data_num = args(0).toString
      val coll_name = "mongodb://192.168.0.10:27017/amazon.SF_"+data_num+"m"
      println(coll_name) 
      val save_coll_name = "mongodb://192.168.0.10:27017/amazon.SF_sig"+data_num+"m"  
      println(save_coll_name) 

      val readConfig = ReadConfig(Map(
        "spark.mongodb.input.uri" -> coll_name,
        "spark.mongodb.input.readPreference.name" -> "primaryPreferred"      
       ))

      val load = MongoSpark.load(sc,readConfig)
      val preRDD = load.map( x => x.getString("reviewText"))

      val dataRDD = preRDD.map(x => (x,x))//.filter(s => (s._1.length() > 50))

      var buildIndexSig = BuildSig.main(sc, dataRDD, 4) // buildIndexSig = tuple4 ( index, f , multiGroup, sc )

      var index = buildIndexSig._1

      var saveIndex = index.map(x =>
            (x._1, x._2._1._1, x._2._1._2, x._2._2))
      
      var paralIndex = saveIndex.map(x => { 
                            new Document().append("signature", x._1).append("inverse", x._2).append("raw", x._3).append("isDel", x._4) 
                          })
      paralIndex.saveToMongoDB(WriteConfig(Map("spark.mongodb.output.uri" -> save_coll_name)))
      
/*
        val indexcoll2 = index.collect()
     val writer2 = new PrintWriter(new File("/home/user/Desktop/hongji/ref/SF_sig1m.json"))
         for(x <- indexcoll2){
           var json = x._2._1._2.toString
           writer2.write(json+"\n")
        }
       writer2.close()
   */    
/*
      
      val data_num = args(0).toString
      val coll_name = "mongodb://192.168.0.11:27017/REVIEW.musical_"+data_num 

      val readConfig = ReadConfig(Map(
        "spark.mongodb.input.uri" -> coll_name,
        "spark.mongodb.input.readPreference.name" -> "primaryPreferred"      
       ))
      val load = MongoSpark.load(sc,readConfig)
      val preRDD = load.map( x => x.getString("reviewText"))
      val dataRDD = preRDD.map(x => (x,x))

      var buildIndexSig = BuildSig.main(sc, dataRDD) // buildIndexSig = tuple4 ( index, f , multiGroup, sc )
      var index = buildIndexSig._1

  
  var saveIndex = index.distinct().map(x =>
   (x._1, x._2._1._1, x._2._1._2, x._2._2))

  //for(i <- index){
      var paralIndex = saveIndex.map(x =>  
                                    {
                                    //Document document = new Document()
                                    new Document().append("signature", x._1).append("inverse", x._2).append("raw", x._3).append("isDel", x._4)
                                    //document
                                    }
                                )
        //sc.parallelize(List(saveIndex.map(x => Document.parse(s"{test: $x._1}"))))
      
      //MongoSpark.save(paralIndex,writeConfig)
      paralIndex.saveToMongoDB(WriteConfig(Map("spark.mongodb.output.uri" -> "mongodb://192.168.0.11/REVIEW.musical_test")))
  //}

  //saveIndex.saveToMongoDB(WriteConfig(Map("spark.mongodb.output.uri" -> "mongodb://mongodb://192.168.0.11/REVIEW.musical_test")))


 
  val reduceIndex = index.distinct()//reduceByKey((v1, v2) => v1 )
  val indexcoll = reduceIndex.collect()
   
  //val fcoll = f.collect()
  //val multicoll = multiGroup.value.collect()

  val writer = new PrintWriter(new File("./buildindex_result"))
  
  for(x <- indexcoll){
    var json = 
         ("signature" -> x._1) ~   
         ("inverse" -> x._2._1._1) ~
         ("raw" -> x._2._1._2) ~
         ("isDel" -> x._2._2)


    val print = compact(render(json))
    writer.write(print+"\n")
  }
  index.unpersist()
  writer.close()



  for(x <- fcoll){
      var json = 
         ("frequency" -> x._2) ~  
         ("signature" -> x._1._1) ~
         ("isDel" -> x._1._2)

    println(compact(render(json)))
  }

  for(x <- multicoll){
    var json = 
         ("signature" -> x._1) ~   
         ("inverse" -> x._2._1._1) ~
         ("raw" -> x._2._1._2) ~
         ("isDel" -> x._2._2))

    println(compact(render(json)))
  }
  */


    }
  }