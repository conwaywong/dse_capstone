package org.ucsd.dse.capstone.anomaly.deprecated

/* Other UCSD Utilities */
import org.ucsd.dse.capstone.traffic.MLibUtils

/* Spark libraries */
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.util.StatCounter

/* Breeze Math imports */
import breeze.linalg.{ DenseVector => BDV }
import breeze.linalg.{ DenseMatrix => BDM }
import breeze.linalg.inv
import breeze.linalg.sum
import breeze.linalg.Axis
import breeze.numerics.sqrt

import java.lang.Math.max

class MahalanobisOutlier(sc:SparkContext) extends AnomalyDetector
{
    private var _sig:BDM[Double] = null
    private var _sig_i:BDM[Double] = null
    private var _location:Vector = null
    private var _fit:Boolean = false

    def fit(X: RDD[Vector])
    {
        _location = Statistics.colStats(X).mean
        _sig = calc_cov(X)
        _sig_i = inv(_sig)
        _fit = true
    }
    
    def mahalanobis(obso: RDD[(Array[Int], BDV[Double])]) : RDD[(Array[Int], Double)] =
    {
        require(_fit, "Must call fit before calculating mahalanobis distance")
        
        /* Broadcast out the center (location) and co-variance matrix (sig) */
        val Blocation:Broadcast[BDV[Double]] = sc.broadcast(BDV(_location.toArray))
        val Bsig_i:Broadcast[BDM[Double]] = sc.broadcast(_sig_i)
        
        obso.map{ case(i,o)=> 
            (i, sqrt(sum((o-Blocation.value).asDenseMatrix*Bsig_i.value*(o-Blocation.value).asDenseMatrix.t, Axis._1).valueAt(0)))}
    }

    def DetectOutlier(X : RDD[(Array[Int], Vector)], stdd:Double = 1.0) : RDD[(Array[Int], Vector)] = 
    {
        require(_fit, "Must call fit before attempting to Detect Outliers")
        
        val Blocation:Broadcast[BDV[Double]] = sc.broadcast(BDV(_location.toArray))
        val mahalanobis_dist:RDD[(Array[Int], Double)] = mahalanobis(X.map{ case(i,o)=>(i, BDV(o.toArray)-Blocation.value) })

        val stats:StatCounter = mahalanobis_dist.map{case(i,o)=>o}.stats()
        
        /* Figure out what the threshold value is based on std-dev */
        val threshold:Double = stats.mean + (stats.variance*stdd)
        val Bthreshold:Broadcast[Double] = sc.broadcast(threshold)
        
        /* Could change this to be a join, but as the number of outliers SHOULD be small it's faster
         * to just collect the results and just let them be broadcasted for the final filtering.
         */
        val outlier:Array[Array[Int]] = mahalanobis_dist.filter{ case(i,o)=>o > Bthreshold.value }.map{ case(i,o)=>i }.collect()
        val Boutlier:Broadcast[Array[Array[Int]]] = sc.broadcast(outlier)
        X.filter { case(i,o) =>
            var found:Boolean = false
            var j:Int = 0
            while(j < Boutlier.value.length && !found)
            {
                if(Boutlier.value(j).sameElements(i))
                    found = true
                j += 1
            }
            found
        }
    }
    
    private def calc_cov(m_vector_rdd: RDD[Vector]) : BDM[Double] =
    {
        val row_matrix:RowMatrix = new RowMatrix(m_vector_rdd)
        MLibUtils.toBreeze(row_matrix.computeCovariance())
    }
}