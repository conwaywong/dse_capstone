package org.ucsd.dse.capstone.traffic

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client

object Test extends App {
  //  val m_matrix: Matrix = Matrices.randn(2, 3, new Random())
  //  println(m_matrix)
  //  //
  //  println()
  //  //
  //  val bm: BDM[Double] = MLibUtils.toBreeze(m_matrix)
  //  val result: BDM[Double] = bm(::, 0 to 1)
  //  val dm = MLibUtils.fromBreeze(result)
  //  println("hello=" + dm)
  //  //
  //  //  val arr= result.toArray
  //  //  Matrices.
  //  //  val smatrix = MLibUtils.fromBreeze(result)
  //  //  println(smatrix)
  //  //
  //  val m_double_arr: Array[Double] = new Array[Double](10)
  //  Arrays.fill(m_double_arr, 3.5)
  //  val m_vector = Vectors.dense(m_double_arr)
  //  println(m_vector)
  //  val breeze_vector = MLibUtils.toBreeze(m_vector)
  //  println(breeze_vector)
  //  println()
  //  breeze_vector -= 1.5
  //  println(breeze_vector)

  //
  val output_aws_id = null // replace with access id
  val output_aws_secret_key = null // replace with secret key
  val cred: AWSCredentials = new BasicAWSCredentials(output_aws_id, output_aws_secret_key)
  val client: AmazonS3 = new AmazonS3Client(cred)
  val bucket_name: String = "dse-team2-2014"
  val file_key = "output/total_flow_eigenvectors.CA_2010.csv"
  //
  val a = IOUtils.read_matrix(client, bucket_name, file_key)
  println(a)
  //
  val vector_file_key= "output/total_flow_mean_vector.CA_2010.csv"
  val b = IOUtils.read_vectors(client, bucket_name, vector_file_key)(0)
  println()
  println(b)
}