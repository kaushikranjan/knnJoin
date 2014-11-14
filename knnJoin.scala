
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Vector
import scala.math._


object knnJoin {

   /**
   * Computes the nearest neighbors in the data-set for the data-point against which KNN
   * has to be applied for A SINGLE ITERATION
   * 
   * @param rdd : RDD of Vectors of String, which is the data-set in which knnJoin has to be undertaken
   * @param data_point : Vector of String, which is the data-point with which knnJoin is done with the data-set
   * @param rand_point : Vector of String, it's the random vector generated in each iteration
   * @param len : the number of data-points from the data-set on which knnJoin is to be done
   * @param zScore : RDD of (Long,Double), which is the <key,zscore> for each entry of the dataset
   * @param data_score : Double value of z-score of the data-point
   * @return an RDD of the nearest 2*len entries from the data-point on which KNN needs to be undertaken for that iteration
   */
  def knnJoin_perIteration(rdd : RDD[Vector[String]],
      data_point : Vector[String],
      rand_point : Vector[String],
      len : Int,
      zScore : RDD[(Long,Double)],
      data_score : Double ) : RDD[Vector[String]] = {
    
  
   // rdd with score greater than the z-score of the data-point
   val greaterRDD = zScore.filter(word  => word._2 > data_score).
   					map(word => word._2 -> word._1).
   					sortByKey(true).
   					map(word => word._2).
   					zipWithIndex.
   					filter(word => word._2 < len).
   					map(word => word._1)
   					
   // rdd with score lesser than the z-score of the data-point
   val lesserRDD = zScore.filter(word => word._2 < data_score).
   							map(word => word._2 -> word._1).
   							sortByKey(false).
   							map(word => word._2).
   							zipWithIndex.
   							filter(word => word._2 < len).
   							map(word => word._1)

   val trim = greaterRDD.union(lesserRDD)
   
   //the keys are mapped to <key,0> to make it a pairRDD for simplicity of further operations
   val join = rdd.zipWithIndex.
   					map(word => word._2 -> word._1).
   					join(trim.map(word => word -> 0)).
   					map(word => word._2._1)
   join
   
  }
  
  
  /**
   * Computes the nearest neighbors in the data-set for the data-point against which KNN
   * has to be applied
   * 
   * @param rdd : RDD of Vectors of String, which is the data-set in which knnJoin has to be undertaken
   * @param data_point : Vector of String, which is the data-point with which knnJoin is done with the data-set
   * @param len : the number of data-points from the data-set on which knnJoin is to be done
   * @param random_size : the number of iterations which has to be carried out
   * @sc : SparkContext
   * @return an RDD of Vectors of String on which simple KNN needs to be applied with respect to the data-point
   */
  def knnJoin(rdd : RDD[Vector[String]], 
      data_point : Vector[String], 
      len : Int, 
      random_size : Int,
      sc : SparkContext)
  : RDD[Vector[String]] = {
   
    val size = rdd.first.length
    val rand = new Array[Double](size)//mutable.Buffer
   
  
   //compute z-scores for each iteration, this being the first
   val model = zScore.computeScore(rdd)
   val data_score = zScore.computeDataScore(data_point, model.mean, model.variance)
   
   //for first iteration rand vector is a ZERO vector
   for(count <- 0 to size-1) rand(count) = 0.0
   
   //compute nearest neighbours on basis of z-scores
   val c_i = knnJoin_perIteration(rdd, data_point, rand.toList.map(word => word.toString).toVector ,len,model.score, data_score)
   c_i.persist
   

   //compute -> rdd where data-set generated from each iteration is being recursively appended 
   var compute = c_i
   compute.persist
   
   
   //the no.of iterations to be performed
   for(coun <- 2 to random_size) {
     
     for(i <- 0 to size-1) rand(i) = random
     
    		 
     //increment each element of the dataset with the random vector "rand"
     var kLooped = -1
     val newRDD = rdd.map(vector => vector.
         				map(word => (word.toDouble + rand({kLooped = kLooped+1
         				kLooped%size})).toString
         			  ))
   
     //compute z-scores for the iteration
     val modelLooped = zScore.computeScore(newRDD)
     val data_scoreLooped = zScore.computeDataScore(data_point, modelLooped.mean, modelLooped.variance)
     
     //compute nearest neighbours on basis of z-scores     
     val c_iLooped = knnJoin_perIteration(newRDD, data_point, rand.toList.map(word => word.toString).toVector, len, modelLooped.score, data_score)
     
     c_iLooped.persist
 
     
     //subtract the effect of random vector from the first entry of the above computed nearest neighbours
     val firstVector_Looped = c_iLooped.first
     var j_Looped = -1
     val temp_newVectorLooped = firstVector_Looped.map(word => (word.toDouble - rand({j_Looped = j_Looped+1
        j_Looped})).toString)
     val newFirstVector_Looped = sc.parallelize(Vector[Vector[String]](temp_newVectorLooped))
     
     newFirstVector_Looped.persist
     
 
     //remove the first entry from the data-set returned by knnJoin_perIteration 
     val c_i2Looped = c_iLooped.zipWithIndex.
     					map(word => word._2 -> word._1).
     					filter(word => word._1 > 0).
     					map(word => word._2)
     c_i2Looped.persist
     

     //append result to compute 
     compute = compute.union(c_i2Looped).union(newFirstVector_Looped)
     compute.persist
   }
   
    
    compute
   
  }
  
}
