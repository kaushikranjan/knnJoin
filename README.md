z-KNN
================================

##What's this? 
Implementation of knn-join presented in http://ieeexplore.ieee.org/xpls/abs_all.jsp?arnumber=5447837&tag=1
It is an approximate knn-Join (K-nearest neighbor) algorithm, which translates each multi-dimensional data-point into a single dimension. KNN search for a data-point can be performed on this single dimension data.
Given a data-set,the algorithm computes the z-values for each entry of the data-set and selects those entries with z-values closest to the z-value of the data-point. The process is performed over multiple iterations using random vector to transform the data-set.By using the data-entries over z-values, kNN is applied to the reduced data-set.

##How to Run
You should have spark already build as a jar file in your build library path.From your main call the function "knnJoin" of this class, with following parameters
```
val model = knnJoin.knnJoin(dataSet : RDD[Vector[Int]], 
                            dataPoint : Vector[Int], 
                            len : Int, 
                            iteration : Int, 
                            sc : SparkContext)

model : RDD(Vector[Int])
```
It contains the kNN over the union of the all selected entried from the data-set as mentioned in the paper


