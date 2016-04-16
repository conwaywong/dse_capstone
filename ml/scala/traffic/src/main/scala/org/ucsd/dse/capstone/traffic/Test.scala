package org.ucsd.dse.capstone.traffic

import java.util.Arrays

import scala.collection.mutable.ListBuffer
import scala.util.Random

object Test {
  def main(args: Array[String]): Unit = {
    val num_clusters = 10

    val error: ListBuffer[Double] = new ListBuffer[Double]()
    for (k <- 0 to num_clusters - 1) {
      error += Random.nextDouble()
    }

    val new_error = error.toArray.map { d =>
      val t = Math.pow(10.0, 11.0)
      d / t
    }

    println(Arrays.toString(new_error))

    val new_error_diff: ListBuffer[Double] = new ListBuffer[Double]()
    for (i <- 1 to num_clusters - 1) {
      val cur = new_error(i)
      val prev = new_error(i - 1)
      println(s"i=$i; prev=$prev; cur=$cur")
    }
  }
}