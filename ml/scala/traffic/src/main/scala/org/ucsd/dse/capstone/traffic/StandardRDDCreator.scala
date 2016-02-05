package org.ucsd.dse.capstone.traffic

import com.amazonaws.services.s3.AmazonS3
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

/**
 * @author dyerke
 */
class StandardRDDCreator(pivot_field: Int) extends RDDCreator {

  val m_pivot_field = pivot_field

  override def new_rdd(sc: SparkContext, client: AmazonS3): RDD[Vector] = {
    //    val aws_id = "AKIAJXVOXFQI22U4QZKQ"
    //    val aws_secret_key = "RHE77VNg9LU0tBTGe9bXvwVZcULx2hiikpfOE2O4".replace("/", "%2F")
    //    val bucket_name = "dse-team2-2014"
    //    val obj_key = "dse_traffic/station_5min/2010/d12/d12_text_station_5min_2010_01_01.txt.gz"
    //    val url = "s3n://%s:%s@%s/%s".format(aws_id, aws_secret_key, bucket_name, obj_key)
    //    val files: List[String] = List(url)
    //    val files: List[String] = List("/home/dyerke/Documents/DSE/capstone_project/traffic/data/01_2010", "/home/dyerke/Documents/DSE/capstone_project/traffic/data/01_2010_first_seven_days")
//    val files: List[String] = List("/home/dyerke/Documents/DSE/capstone_project/traffic/data/d11_text_station_5min_2015_01_01_mod.txt")
//    val handler: PivotHandler[Vector] = new StandardPivotHandler(sc, m_pivot_field)
//    MLibUtils.pivot(MLibUtils.new_rdd(sc, files, 4), handler)
    null
  }
}