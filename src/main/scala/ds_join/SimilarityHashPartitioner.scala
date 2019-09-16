package ds_join

import org.apache.spark.Partitioner
import org.apache.spark.broadcast.Broadcast

/*SimilarityHashPartitioner calss*/

class SimilarityHashPartitioner(numParts: Int,
                                frequencyTable: Broadcast[scala.collection.immutable.Map[Int, Int]]
                               ) extends Partitioner {

  def numPartitions: Int = numParts
  def hashStrategy(key: Any): Int = {
    val code = (key.hashCode % numPartitions)
    if (code < 0) {
      code + numPartitions
    } else {
      code
    }
  }
 def getPartition(key: Any): Int = {
    val k = key.hashCode()
    frequencyTable.value.getOrElse(k, hashStrategy(k))
  }
  override def equals(other: Any): Boolean = other match {
    case similarity: SimilarityHashPartitioner =>
      similarity.numPartitions == numPartitions
    case _ =>
      false
  }
 override def hashCode: Int = numPartitions
}