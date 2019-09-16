import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object wordcount{
  def main(args: Array[String]){

  val conf = new SparkConf().setAppName("wordCount")

  val sc = new SparkContext(conf)

  val input = sc.textFile("./README.md")

  val words = input.flatMap(line => line.split(" "))

  val counts = words.map(word => (word,1)).reduceByKey{case (x,y) => x+y}

  counts.saveAsTextFile("output")
  }
}
