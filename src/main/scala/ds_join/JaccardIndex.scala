package ds_join
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable


/*JaccardIndex class*/
case class Extra(frequencyTable: Broadcast[scala.collection.Map[(Int, Boolean), Long]],
  multiGroup: Broadcast[Array[(Int, Int)]], minimum: Int, alpha: Double, partitionNum: Int)

class JaccardIndex() extends Index with Serializable{
  val index = scala.collection.mutable.Map[Int, List[Int]]()
  var threshold = 0.0
  var extra: Extra = null

  def CalculateH ( l: Int, s: Int, threshold: Double ): Int = {
    Math.floor((1 - threshold) * (l + s) / (1 + threshold) + 0.0001).toInt + 1
  }
  def CalculateH1 ( l: Int, threshold: Double ): Int = {
    Math.floor ( (1 - threshold) * l / threshold + 0.0001).toInt + 1
  }

  def calculateOverlapBound(t: Float, xl: Int, yl: Int): Int = {
    (Math.ceil((t / (t + 1)) * (xl + yl)) + 0.0001).toInt
  }

  def verify(x: Array[(Array[Int], Array[Boolean])],
             y: Array[(Array[Int], Array[Boolean])],
             threshold: Double,
             pos: Int
            ): Boolean = {

    var xLength = 0
    for (i <- x) {
      xLength += i._1.length
    }
    var yLength = 0
    for (i <- y) {
      yLength += i._1.length
    }
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
          return false
        }
      }// before matching
      if (currentOverlap + Math.min((xLength - currentXLength),
        (yLength - currentYLength)) < overlap) {
        return false
      }
    }
    if (currentOverlap >= overlap) {
      return true
    } else {
      return false
    }
  }

  def compareSimilarity(query: (Array[(Array[Int], Array[Boolean])],
    Boolean, Array[Boolean], Boolean, Int), index: (Array[(Array[Int], Array[Boolean])],
    Boolean)): Boolean = {
    val pos = query._5
    if ((query._3.length > 0 && query._3(0)) ||
      (query._3.length > 0 && !query._3(0) && !index._2)) {
      verify(query._1, index._1, threshold, pos)
    } else {
      false
    }
  }

  def createInverse(ss1: String,
                    group: Array[(Int, Int)],
                    threshold: Double
                   ): Array[(String, Int, Int)] = {
    {
      val ss = ss1.split(" ")
      val range = group.filter(
        x => (x._1 <= ss.length && x._2 >= ss.length)
      )
      val sl = range(range.length-1)._1
      val H = CalculateH1(sl, threshold)
      for (i <- 1 until H + 1) yield {
        val s = ss.filter(x => {x.hashCode % H + 1 == i})
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

  def init(threshold: Double,
    frequencyT: Broadcast[scala.collection.Map[(Int, Boolean), Long]],
    multiG: Broadcast[Array[(Int, Int)]],
    minimum: Int,
    alpha: Double,
    partitionNum: Int): Unit = {
    this.threshold = threshold
    this.extra = Extra(frequencyT, multiG, minimum, alpha, partitionNum)
  }

  def addIndex(key: Int, position: Int): Unit = {
    index += (key -> (position :: index.getOrElse(key, List())))
  }

  def sampleSelectivity(data: Array[((Int, String,
                              Array[(Array[Int], Array[Boolean])]), Boolean)],
                        key: Array[(Array[(Array[Int], Array[Boolean])],
                          Array[(Int, Boolean, Array[Boolean], Boolean, Int)])],
                        t: Double, sampleRate: Double): Double = {
    val sampledKey: Seq[(Array[(Array[Int], Array[Boolean])],
      Array[(Int, Boolean, Array[Boolean], Boolean, Int)])] = {
      for (i <- 1 to math.max(1, (sampleRate * key.length).toInt);
           r = (Math.random * key.size).toInt) yield key(r)
    }
    var selectivity = (0, 0.0)
    for (query <- sampledKey) {
      for (i <- query._2) {
        val positionSize = index.getOrElse(i._1, List()).length.toDouble
        selectivity = (selectivity._1 + 1, selectivity._2 + positionSize / data.size)
      }
    }

    selectivity._2 / math.max(1.0, selectivity._1)
  }

  def findIndex(data: Array[((Int, String, Array[(Array[Int], Array[Boolean])]), Boolean)],
                key: Array[(Array[(Array[Int], Array[Boolean])],
                  Array[(Int, Boolean, Array[Boolean], Boolean, Int)])],
                t: Double): Array[String] = {

    val ans = mutable.ListBuffer[String]()

    for (query <- key) {
      for (i <- query._2) {
        val position = index.getOrElse(i._1, List())
        for (p <- position) {
//          println(s"Found in Index")
          val que = (query._1, i._2, i._3, i._4, i._5)
          val Ind = (data(p)._1._3, data(p)._2)
          if (compareSimilarity(que, Ind)) {
            ans += data(p)._1._2
          }
        }
      }
    }
    ans.toArray
  }

  def sequentialScan(data: Array[((Int, String,
                          Array[(Array[Int], Array[Boolean])]), Boolean)],
                key: Array[(Array[(Array[Int], Array[Boolean])],
                  Array[(Int, Boolean, Array[Boolean], Boolean, Int)])],
                t: Double): Array[String] = {

    val ans = mutable.ListBuffer[String]()

    for (query <- key) {
      for (i <- query._2) {
        for (entry <- data) {
          val que = (query._1, i._2, i._3, i._4, i._5)
          val Ind = (entry._1._3, entry._2)
          if (compareSimilarity(que, Ind)) {
            ans += entry._1._2
          }
        }
      }
    }
    ans.toArray
  }
}


/*JaccardIndex object*/
object JaccardIndex {
  def apply(data: Array[(Int, ((String, String), Boolean))],
            threshold: Double,
            frequencyT: Broadcast[scala.collection.Map[(Int, Boolean), Long]],
            multiG: Broadcast[Array[(Int, Int)]],
            minimum: Int,
            alpha: Double,
            partitionNum: Int): Index = {
    val res = new JaccardIndex()
    res.init(threshold, frequencyT, multiG, minimum, alpha, partitionNum)
    for (i <- 0 until data.length) {
      res.addIndex(data(i)._1, i)
    }
    res
  }
}
