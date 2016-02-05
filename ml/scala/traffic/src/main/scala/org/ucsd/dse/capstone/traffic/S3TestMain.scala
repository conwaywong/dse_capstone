package org.ucsd.dse.capstone.traffic

import java.io.BufferedWriter
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStreamWriter
import java.io.PrintWriter

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ObjectMetadata

/**
 * @author dyerke
 */
object S3TestMain {

  def main(args: Array[String]) {
    val template: SparkTemplate = new DefaultSparkTemplate()
    template.execute { sc => do_execute(sc) }
  }

  def do_execute(sc: SparkContext) = {

    val aws_id = "AKIAJFOAU7QJI4CFLAKA"
    val aws_secret_key = "6bBcmynL6SB0OeVX4+TUGecpQURjMu5cNqmqTaOi".replace("/", "%2F")
    val bucket_name = "dse-jgilliii"
    val obj_key = "dse_traffic/station_5min/2010/d12/d12_text_station_5min_2010_01_01.txt.gz"
    val url = "s3n://%s:%s@%s/%s".format(aws_id, aws_secret_key, bucket_name, obj_key)

    println("Attempting to acccess url " + url)

    val files: List[String] = List(url)
    val m_string_rdd: RDD[String] = sc.textFile(files.mkString(","))

    val test: Array[String] = m_string_rdd.take(10)

    val stream: ByteArrayOutputStream = new ByteArrayOutputStream();
    val pw: PrintWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(stream)))
    try {
      test.foreach(pw.println(_))
    } finally {
      pw.close()
    }
    val bytes: Array[Byte] = stream.toByteArray()
    println("test= " + new String(bytes))
    //
    // write to S3
    //
    //    val output_aws_id = "AKIAIR3CW5CUFISNJK3Q"
    //    val output_aws_secret_key = "RV6/L9HcNozZd009BsKeVmFFUL2ItixPUffXJWTh".replace("/", "%2F")
    //    val output_bucket_name = "dse-kdyer2"
    //    val output_bucket_key = "tmp/b.csv"
    val output_aws_id = "AKIAJFOAU7QJI4CFLAKA"
    val output_aws_secret_key = "6bBcmynL6SB0OeVX4+TUGecpQURjMu5cNqmqTaOi".replace("/", "%2F")
    val output_bucket_name = "dse-csw009"
    val output_bucket_key = "upload/b.csv"
    //
    val cred: AWSCredentials = new BasicAWSCredentials(output_aws_id, output_aws_secret_key)
    val client: AmazonS3 = new AmazonS3Client(cred)
    val in_stream: InputStream = new ByteArrayInputStream(bytes)
    val in_stream_meta: ObjectMetadata = new ObjectMetadata()
    in_stream_meta.setContentLength(bytes.length)
    client.putObject(output_bucket_name, output_bucket_key, in_stream, in_stream_meta)
  }
}