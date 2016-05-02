import java.lang.Math
import scala.util.Random

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object Sort {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Sort")
        val sc = new SparkContext(conf)
        args.toList match {
            case amount::loc::Nil => {
                val gigs = "([0-9]+)g$".r
                val sz = amount match {
                    case gigs(num) => Integer.parseInt(num) * 1e9
                    case _ => throw new Exception("Non-gig amounts not supported")
                }
                val randomInts = gen(sc, sz.toInt)
                val sorted = sort(randomInts)
                sorted.saveAsTextFile(loc)
            }
            case _ => {
                println("Usage: spark-submit ... Sort (amount) (output)")
                sc.stop()
            }
        }
    }

    def gen(sc: SparkContext, amount: Int): RDD[Int] = {
        // amount:  bytes
        val int_size = 4.0
        val numOfInts = Math.floor(amount / int_size).toInt
        return sc.parallelize(1 to numOfInts).map(_ => scala.util.Random.nextInt)
    }

    def sort(inp: RDD[Int]): RDD[Int] = {
        inp.sortBy(i => i, true)
    }
}
