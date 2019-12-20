/* for similairtyRDD -> indexPartitionedRDD */
package ds_join

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import org.apache.spark.{Partition, TaskContext}

class SimilarityRDD[U: ClassTag](prev: RDD[U], preservesPartitioning: Boolean) extends RDD[U](prev){

  val nodeIPs = Array (
    "user-17",
    "user-19",
    //"user-231",
    "user-243",
    //"user-234",
    "user-244"
    //"user-232",
    //"user-233"
  )
  override def getPreferredLocations(split: Partition): Seq[String] =
    Seq(nodeIPs(split.index % nodeIPs.length))

  override val partitioner = if (preservesPartitioning) firstParent[U].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[U].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[U] =
    prev.compute(split, context)
}
