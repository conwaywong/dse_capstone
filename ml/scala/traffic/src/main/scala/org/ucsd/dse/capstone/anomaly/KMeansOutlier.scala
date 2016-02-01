package org.ucsd.dse.capstone.anomaly

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import breeze.linalg.squaredDistance
import breeze.linalg.{DenseVector => BDV}

// Example KMeans in Scala Spark
//     - https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/SparkKMeans.scala
class KMeansOutlier(sc:SparkContext, numClust:Int, numOutlier:Int, convergeDist:Double) extends AnomalyDetector
{
    private val _k:Int = numClust
    private val _l:Int = numOutlier
    private val _cD:Double = convergeDist
    //private var L = []
    //private var XI = []
    private var C:Array[Vector] = null
    private var _i:Int = 0
    private val _sc:SparkContext = sc
    
    /* Utility functions
    private def _centI(x, C) : Int =
    {
        return np.argmin([sdist.euclidean(x, c) for c in C])
    }

    private def _cent(x, C) //: =
    {
        return C[_centI(x,C)]
    }

    private def _dist(x, C) //: =
    {
        return sdist.euclidean(x, _cent(x, C))
    }
    */
    
    def fit(X: RDD[Vector])
    {
        /*
        self.L.append([])
        self.XI.append(X)
        self.C.append(self.XI[0][np.random.choice(X.shape[0], self._k, replace=False)])
         */
        _i = 1
    }
    
    def DetectOutlier(X : RDD[(Vector, Long)], stdd:Double = 1.0) : RDD[Vector] =
    {
        X.map{ case(o,i)=>o }
        /*convergence = False
        while not convergence:
            # Compute d(x | Ci-1) for all x in X
            # Re-order the points in X by decreasing distance
            XO = self.XI[0][np.argsort([self._dist(x, self.C[self._i-1]) for x in self.XI[0]])[::-1]]
            # Save off l 'outliers'
            self.L.append(XO[:self._l])
            # Generate new list of points
            self.XI.append(XO[self._l:])
            # Assign points to centers
            P = {j:[] for j in xrange(self._k)}
            for x in self.XI[self._i]:
                P[self._centI(x, self.C[self._i-1])].append(x)
            # Calculate new centers
            self.C.append(np.array([np.mean(P[j],0) for j in xrange(self._k)]))
            if np.linalg.norm(self.C[self._i-1] - self.C[self._i]) < self._cD:
                convergence = True
            else:
                self._i += 1
        return self.L[self._i]*/
    }
}