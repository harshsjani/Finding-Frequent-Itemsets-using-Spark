import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.util.control.Breaks._

import collection.mutable.{ArrayBuffer, HashMap, HashSet}
import java.io.{File, PrintWriter}
import scala.io.Source
import scala.math.Ordering.Implicits._

object task1 {
  val BUCKET_COUNT = 32768

  def format_data(data: Array[List[String]]): String = {
    var dtext = ""
    var ml = 1

    for (x <- data) {
      val l = x.length

      if (l > ml) {
        dtext = dtext.stripSuffix(",")
        dtext += "\n\n"
        ml = l
      }

      dtext += "("

      for (i <- x) {
        dtext += "'" + i  + "'" + ", "
      }

      dtext = dtext.stripSuffix(", ")
      dtext += "),"
    }

    return dtext.stripSuffix(",")
  }

  def write_data(output_file: String, cand: Array[List[String]], freq: Array[List[String]]) = {
    var text = ""
    text += "Candidates:\n"
    text += format_data(cand)
    text += "\n\n"
    text += "Frequent Itemsets:\n"
    text += format_data(freq).stripSuffix("\n\n")

    val out = new PrintWriter(new File(output_file))
    out.write(text)
    out.close()
  }

  def get_L1(baskets: Array[Array[String]], s: Int): Set[String] = {
    val counts = new HashMap[String, Int]().withDefaultValue(0)
    var L1 = new HashSet[String]()

    for (basket <- baskets) {
      for (item <- basket) {
        counts(item) += 1
      }
    }

    for (item <- counts) {
      if (item._2 >= s) {
        L1 += item._1
      }
    }

    return L1.toSet
  }

  def getCFromL(baskets: Array[Array[String]], L: Set[String], k: Int): Map[Set[String], Int] = {
    val C = new HashMap[Set[String], Int]().withDefaultValue(0)

    for (basket <- baskets) {
      for (item <- basket.toSet.intersect(L).toArray.sorted.combinations(k)) {
        C(item.toSet) += 1
      }
    }
    return C.toMap
  }

  def getLFromC(C: Map[Set[String], Int], s: Int): Array[Array[String]] = {
    val L = new ArrayBuffer[Array[String]]()

    for (item <- C) {
      if (item._2 >= s) {
        L += item._1.toArray
      }
    }
    return L.toArray
  }

  def multihash(baskets: Array[Array[String]], s: Int, totalItems: Long): Iterator[Set[String]] = {
    val support = math.ceil(s * baskets.length / totalItems.toFloat).toInt
    println("Support: " + support)

    var k = 2

    val l = get_L1(baskets, support)
    var frequentCandidates = new ArrayBuffer[Set[String]]()

    for (x <- l.toArray.sorted) {
      frequentCandidates += HashSet(x).toSet
    }

    var temp = l

    breakable {
      while (true) {
        val ckp1 = getCFromL(baskets, temp, k)
        println("C for k="+k)
        val lkp1 = getLFromC(ckp1, support)

        if (lkp1.length == 0) {
          break()
        }

        for (x <- lkp1) {
          frequentCandidates += x.sorted.toSet
        }

        var t2 = new HashSet[String]()
        for (x <- lkp1) {
          t2 = t2.union(x.toSet)
        }
        temp = t2.toSet
        k += 1
      }
    }
    return frequentCandidates.toList.toIterator
  }

  def SONp2(baskets: Array[Array[String]], candidates: Array[Set[String]]): Iterator[Tuple2[Set[String], Int]] = {
    val ctr = new HashMap[Set[String], Int]().withDefaultValue(0)

    for (basket <- baskets) {
      val bk = basket.toSet
      for (candidate <- candidates) {
        if (candidate.toArray.length > basket.length) {
          // Figure this out later
        }
        if (candidate.subsetOf(bk)) {
          ctr(candidate) += 1
        }
      }
    }
    val ret = ArrayBuffer[Tuple2[Set[String], Int]]()

    for (item <- ctr) {
      ret += Tuple2[Set[String], Int](item._1, item._2)
    }
    return ret.toArray.toIterator
  }

  def main(args: Array[String]): Unit = {
    val sc = SparkContext.getOrCreate()
    sc.setLogLevel("OFF")

    val taskcase = args(0).toInt - 1
    val support = args(1).toInt
    val input_file = args(2)
    val output_file = args(3)

    val st = System.nanoTime

    val textRDD = sc.textFile(input_file)
    val first = textRDD.first()
    val rdd = textRDD.filter(x => x != first)
      .map(x => (x.split(",")(taskcase % 2), x.split(",")(math.abs(~taskcase % 2))))
      .groupByKey()
      .map(x => x._2.toSet.toArray)

    val totalItems = rdd.count()

    val candidates = rdd.mapPartitions(chunk => multihash(chunk.toArray, support, totalItems))
      .distinct()
      .sortBy(x => (x.toList.size, x.toList))
      .collect()

    println("Number of candidate sets: " + candidates.length)

    val frequentItemsets = rdd.mapPartitions(chunk => SONp2(chunk.toArray, candidates))
      .reduceByKey((x, y) => x + y)
      .filter(x => x._2  >= support)
      .map(x => x._1)
      .distinct()
      .map(x => x.toList.sorted)
      .sortBy(x => (x.size, x))
      .collect()

    println("Number of frequent sets: " + frequentItemsets.length)
    val cd = candidates.map(x => x.toList)
    write_data(output_file, cd, frequentItemsets)

    val et = (System.nanoTime - st) / 1e9d
    println("Duration: " + et)
  }
}
