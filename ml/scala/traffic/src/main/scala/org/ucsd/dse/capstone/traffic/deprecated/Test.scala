package org.ucsd.dse.capstone.traffic.deprecated

import java.util.Arrays

object Test {
  def main(args: Array[String]): Unit = {
    //    val num_clusters = 10
    //
    //    val error: ListBuffer[Double] = new ListBuffer[Double]()
    //    for (k <- 0 to num_clusters - 1) {
    //      error += Random.nextDouble()
    //    }
    //
    //    val new_error = error.toArray.map { d =>
    //      val t = Math.pow(10.0, 11.0)
    //      d / t
    //    }
    //
    //    println(Arrays.toString(new_error))
    //
    //    val new_error_diff: ListBuffer[Double] = new ListBuffer[Double]()
    //    for (i <- 1 to num_clusters - 1) {
    //      val cur = new_error(i)
    //      val prev = new_error(i - 1)
    //      println(s"i=$i; prev=$prev; cur=$cur")
    //    }
    val m_arr2: Array[Int]= Array(2351, 2322)
    val m_arr: Array[Int]= Array(2351, 2322, 2479, 15683, 17240, 17241, 17252)
    
    for (i <- m_arr) {
      if (m_arr2.contains(i)) {
        println(i)
      }
    }
    
//    val list = List[String]("400980.0", "4.0", "2008.0", "181.0", "1.0", "1.0", "356.52817656991203", "-782.2868325925001", "-365.11562268413456", "-352.1000085644429", "35.773181930762505", "-277.3475326461268", "-206.0180102739048", "-22.605349857653337", "81.31079486429351", "-100.1686786300935")
//    val arr: Array[Double] = list.toArray.map(_.toDouble)
//    println(Arrays.toString(arr))
//    //
//    val new_arr: Array[Double]= arr.slice(6, arr.length)
//    println(Arrays.toString(new_arr))
  }
}