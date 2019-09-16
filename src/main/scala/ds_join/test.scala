package ds_join

//import com.mongodb.spark._
//import com.mongodb.spark.config._
//import com.mongodb.spark.sql._
import org.apache.spark.streaming._
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
// ./bin/spark-shell --packages org.mongodb.scala:mongo-scala-driver_2.11:2.0.0 org.mongodb.scala:mongo-scala-bson_2.11:2.0.0 org.mongodb:bson.0.0/home/user/Desktop/hongji/Dima_Ds_join/target/scala-2.11/Dima-DS-assembly-1.0.jar

object test{

  def main(args: Array[String]){


      val conf = new SparkConf().setAppName("test")
      val sc = new SparkContext(conf)

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
      

     var cacheTmp: org.apache.spark.rdd.RDD[(Int, ((String, String), Boolean))] = null
     var cacheTmp2: org.mongodb.scala.Observable[(Int, ((String, String), Boolean))] = null

     val client: MongoClient = MongoClient("mongodb://192.168.0.11:27017")
     val database: MongoDatabase = client.getDatabase("REVIEW")
     val collection: MongoCollection[Document] = database.getCollection("musical_test")

     var document: Document = Document("signature" -> 616998131)
     
     //document += document("signature" -> 616998131)

     //doc.append()

     var dbData = mutable.ListBuffer[(Int, ((String, String), Boolean))]()
     var document: Array[(Document)] = Array(Document("signature" -> 616998131),Document("signature" -> 2025009520))
     document +:= (Document("dfd"-> 123 ))

     for(x <- document){ collection.find(x).foreach(println) }
    // cacheTmp2 = collection.find(or(equal("signature",616998131),equal("signature",2025009520))).map(x => (x.getInteger("signature").toInt,((x.getString("inverse"), x.getString("raw")), x.getBoolean("isDel"))))
     cacheTmp2 = collection.find(document).map(x => (x.getInteger("signature").toInt,((x.getString("inverse"), x.getString("raw")), x.getBoolean("isDel"))))
     
     //val temp = cacheTmp2.foreach(x => dbData += ((x._1, ((x._2._1._1, x._2._1._2), x._2._2))))
     //println(dbData)
     cacheTmp2.collect().foreach(println)
*/
    }
  }