package org.ucsd.dse.capstone.anomaly.deprecated

import scala.collection.Map

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import breeze.linalg.squaredDistance
import breeze.linalg.{DenseVector => BDV}

// Example KMeans in Scala Spark
//     - https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/SparkKMeans.scala

/* Utility functions */
object KMeansUtil {
    def centI(p:BDV[Double], centers:Array[BDV[Double]]): Int = 
    {
        var bestIndex = 0
        var closest = Double.PositiveInfinity
        
        for (i <- 0 until centers.length)
        {
            val tempDist = squaredDistance(p, centers(i))
            if (tempDist < closest)
            {
                closest = tempDist
                bestIndex = i
            }
        }
        bestIndex
    }
    
    def cent(p:BDV[Double], centers:Array[BDV[Double]]) : BDV[Double] =
    {
        centers(centI(p,centers))
    }
    
    def dist(p:BDV[Double], centers:Array[BDV[Double]]) =
    {
        squaredDistance(p, cent(p, centers))
    }
}
    
class KMeansOutlier(sc:SparkContext, numClust:Int, numOutlier:Int) extends AnomalyDetector
{
    private var _kCent:Array[BDV[Double]] = new Array[BDV[Double]](numClust)
    private var _fit:Boolean = false

    def fit(X: RDD[Vector])
    {
        _kCent = X.map{ x=>BDV(x.toArray) }.takeSample(false, numClust).toArray
        _fit = true
    }
    
    def DetectOutlier(X : RDD[(Array[Int], Vector)], convergeDist:Double) : RDD[(Array[Int], Vector)] =
    {
        require(_fit, "Must call fit before attempting to Detect Outliers")
        
        var outlier:Array[Array[Int]] = null
        var mDist:Double = Double.PositiveInfinity
        var _i:Int = 0
        
        while(mDist > convergeDist)
        {
            _i += 1
            // Compute d(x | Ci-1) for all x in X
            // Broadcast the centers to all nodes
            val Bcent:Broadcast[Array[BDV[Double]]] = sc.broadcast(_kCent)
            val XO:RDD[(Array[Int], Double)] = X.map{ case(i,o)=>(i, KMeansUtil.dist(BDV(o.toArray), Bcent.value)) }
            
            // Re-order the points in X by decreasing distance
            // and save off _l 'outliers'
            outlier = XO.takeOrdered(numOutlier)(Ordering[Double].reverse.on{ case(i,d)=>d }).map{ p=>p._1 }
            // Broadcast list of points to be filtered out for update centers
            val Boutlier:Broadcast[Array[Array[Int]]] = sc.broadcast(outlier)
            
            // Calculate new centers
            val Ncent:Map[Int, BDV[Double]] =
                X.filter{ case(i,o)=>
                    var found:Boolean = false
                    var j:Int = 0
                    while(j < Boutlier.value.length && !found)
                    {
                        if(Boutlier.value(j).sameElements(i))
                            found = true
                        j += 1
                    }
                    !found
                 }
                 .map{ case(i,o)=>(KMeansUtil.centI(BDV(o.toArray), Bcent.value), (BDV(o.toArray),1)) } // the 1 is used for summing
                 .reduceByKey{ case((point1, count1), (point2, count2))=>(point1+point2, count1+count2) } // Add up points/counts
                 .map{ pair=>(pair._1, pair._2._1 * (1.0 / pair._2._2)) }.collectAsMap() // Calculate new average centers and return as map
            // Determine if convergence has occurred
            mDist = 0.0
            for (i <- 0 until numClust)
                mDist += squaredDistance(_kCent(i), Ncent(i))
            
            // Update centers and go again
            for (newP <- Ncent)
                _kCent(newP._1) = newP._2
            println("Interation " + _i + " completed. mDist(" + mDist + ") ? convergeDist(" + convergeDist + ")")
        } 

         // Broadcast list of points to be filtered out for update centers on last time
        val Boutlier:Broadcast[Array[Array[Int]]] = sc.broadcast(outlier)
        X.filter { case(i,o)=>
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
}