package org.ucsd.dse.capstone.traffic

import java.util.Arrays
import java.util.Random

import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.MatrixUDT
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.types.SQLUserDefinedType

import breeze.linalg.{ DenseMatrix => BDM }

object Test extends App {
  val m_matrix: Matrix = Matrices.randn(2, 3, new Random())
  println(m_matrix)
  //
  println()
  //
  val bm: BDM[Double] = MLibUtils.toBreeze(m_matrix)
  val result: BDM[Double] = bm(::, 0 to 1)
  val dm = MLibUtils.fromBreeze(result)
  println("hello=" + dm)
  //
  //  val arr= result.toArray
  //  Matrices.
  //  val smatrix = MLibUtils.fromBreeze(result)
  //  println(smatrix)
  //
  val m_double_arr: Array[Double] = new Array[Double](10)
  Arrays.fill(m_double_arr, 3.5)
  val m_vector = Vectors.dense(m_double_arr)
  println(m_vector)
  val breeze_vector = MLibUtils.toBreeze(m_vector)
  println(breeze_vector)
  println()
  breeze_vector -= 1.5
  println(breeze_vector)
}