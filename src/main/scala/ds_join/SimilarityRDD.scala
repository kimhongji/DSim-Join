/* for similairtyRDD -> indexPartitionedRDD */
package ds_join

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import org.apache.spark.{Partition, TaskContext}

class SimilarityRDD[U: ClassTag](prev: RDD[U], preservesPartitioning: Boolean) extends RDD[U](prev){

  val nodeIPs = Array (
    "teaker-4",
    "teaker-5",
    "teaker-6",
    "teaker-7",
    "teaker-8",
    "teaker-9",
    "teaker-10",
    "teaker-11"
  )
  override def getPreferredLocations(split: Partition): Seq[String] =
    Seq(nodeIPs(split.index % nodeIPs.length))

  override val partitioner = if (preservesPartitioning) firstParent[U].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[U].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[U] =
    prev.compute(split, context)
}
