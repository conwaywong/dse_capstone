package org.ucsd.dse.capstone.anomaly

/* Other UCSD Utilities */
import org.ucsd.dse.capstone.traffic._

/* Spark libraries */
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary => MVSS, Statistics}
import org.apache.spark.util.StatCounter

/* Breeze Math imports */
import breeze.linalg.{ DenseVector => BDV }
import breeze.linalg.{ DenseMatrix => BDM }
import breeze.linalg.inv
import breeze.linalg.sum
import breeze.linalg.Axis
import breeze.numerics.sqrt
import breeze.linalg.argsort
import javax.swing.text.html.CSS.Value

import java.lang.Math.max

class MahalanobisOutlier(sc:SparkContext) extends AnomalyDetector
{
    private var _sig:BDM[Double] = null
    private var _sig_i:BDM[Double] = null
    private var _location:Vector = null
    private val _sc:SparkContext = sc
    private var _fit:Boolean = false

    def fit(X: RDD[Vector])
    {
        _location = Statistics.colStats(X).mean
        _sig = calc_cov(X)
        _sig_i = inv(_sig)
        _fit = true
    }
    
    def mahalanobis(obso: RDD[(BDV[Double], Long)]) : RDD[(Double, Long)] =
    {
        require(_fit, "Must call fit before calculating mahalanobis distance")
        
        /* Broadcast out the center (location) and co-variance matrix (sig) */
        val Blocation:Broadcast[BDV[Double]] = _sc.broadcast(BDV(_location.toArray))
        val Bsig_i:Broadcast[BDM[Double]] = _sc.broadcast(_sig_i)
        
        obso.map{ case(o,i)=> 
            (sqrt(sum((o-Blocation.value).asDenseMatrix*Bsig_i.value*(o-Blocation.value).asDenseMatrix.t, Axis._1).valueAt(0)), i)}
    }

    def DetectOutlier(X : RDD[(Vector,Long)], stdd:Double = 1.0) : RDD[Vector] = 
    {
        require(_fit, "Must call fit before attempting to Detect Outliers")
        
        val Blocation:Broadcast[BDV[Double]] = _sc.broadcast(BDV(_location.toArray))
        val mahalanobis_dist:RDD[(Double, Long)] = mahalanobis(X.map{ case(o,i)=>(BDV(o.toArray)-Blocation.value,i) })

        val stats:StatCounter = mahalanobis_dist.map{case(o,i)=>o}.stats()
        
        /* Figure out what the threshold value is based on std-dev */
        val threshold:Double = stats.mean + (stats.variance*stdd)
        val Bthreshold:Broadcast[Double] = _sc.broadcast(threshold)
        
        /* Could change this to be a join, but as the number of outliers SHOULD be small it's faster
         * to just collect the results and just let them be broadcasted for the final filtering.
         */
        val outlier:Set[Long] = mahalanobis_dist.filter{ case(o,i)=>o > Bthreshold.value }.map{ case(o,i)=>i }.collect().toSet
        val Boutlier:Broadcast[Set[Long]] = _sc.broadcast(outlier)
        X.filter { case(o,i)=>Boutlier.value.contains(i) }.map{ case(o,i)=>o }
    }
    
    private def calc_cov(m_vector_rdd: RDD[Vector]) : BDM[Double] = 
    {
        val row_matrix:RowMatrix = new RowMatrix(m_vector_rdd)
        MLibUtils.toBreeze(row_matrix.computeCovariance())
    }
}