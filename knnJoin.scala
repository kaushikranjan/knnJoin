
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Vector
import scala.math._
import com.google.common.hash._


object knnJoin {
  
   /**
   * Trims the data-set of the non-required entries based on the keys computed in each iteration of KnnJoin
   * 
   * @param rdd : RDD of Vectors of String, which is the data-set in which knnJoin has to be undertaken
   * @param hash : the hash_function applied
   * @param keys : RDD of (Long,Int) containing the keys on basis of which the data-set needs to be cleansed
   * @return an RDD containing data-points matching the keys present in the argument list
   */
  def trimmedData(rdd : RDD[Vector[String]], hash : HashFunction, keys : RDD[(Long,Int)]) : RDD[Vector[String]] = {
   
    //dimension of data-point
    val attributeLength = rdd.first.length
    //no.of data points
    val size = rdd.count
    
    var counter = -1
    val rdd_temp = rdd.map(word =>{
      counter = counter+1
      counter
      } -> word )
   
    //keys.saveAsTextFile("/home/kaushik/Desktop/zScores")
    //rdd_temp.saveAsTextFile("/home/kaushik/Desktop/rdd")
    
        
    /**
     * keys(Long,Int) not required
     * as using array of Long instead
     */
        
    val keyValues = keys.map(word => word._1).collect
   
    //rdd_temp.foreach(word => println("rdd " + word._1))
    //keys.foreach(word => println("keys " + word._1 + " : " + word._2))
    
    //val join = rdd_temp.filter(word => t.contains(word._1 - size)).map(word => word._2)//keys.map(word => brd.value.lookup(word._1)(1))
    val join = rdd_temp.filter(word => keyValues.contains(word._1)).map(word => word._2)
    //join.saveAsTextFile("/home/kaushik/Desktop/joins")
        
    join

    
  }
  
  
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
  def knnJoin_perIteration(rdd : RDD[Vector[String]], data_point : Vector[String], rand_point : Vector[String],
      len : Int, zScore : RDD[(Long,Double)], data_score : Double ) : RDD[Vector[String]] = {
    
    val hash_seed = Hashing.murmur3_128(5) 
    val attributeLength = rdd.first.length
     
    //reset()
    
    var i = -1L
    val rdd_temp = rdd.flatMap(line => line.seq).
   					map(word => ({i=i+1
   					  i
   					  }/attributeLength).toLong -> (word.toDouble + rand_point(i.toInt%attributeLength).toDouble)).
   					groupByKey.sortByKey(true)
   
   var j = -1L
   // rdd with score greater than the z-score of the data-point
   val greaterRDD = zScore.filter(word  => word._2 > data_score).
   					map(word => word._2 -> word._1).
   					sortByKey(true).
   					map(word => word._2).filter(word => {
   					  j = j+1
   					  j
   					  } < len).collect

   var k = -1L
   // rdd with score lesser than the z-score of the data-point
   val lesserRDD = zScore.filter(word => word._2 < data_score).
   					    map(word => word._2 -> word._1).
   					    sortByKey(false).
   					    map(word => word._2).
   					    filter(word => {
   					      k = k+1
   					      k
   					    } < len).collect
   					    

   val sc = new SparkContext("local", "knnJoin2")
   val trim = sc.parallelize(greaterRDD.union(lesserRDD))
   //the keys are mapped to <key,0> to make it a pairRDD for simplicity of further operations
   val keysOfDataPoints = trim.map(word => word -> 0)

   val join = trimmedData(rdd, hash_seed, keysOfDataPoints)
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
   * @return an RDD of Vectors of String on which simple KNN needs to be applied with respect to the data-point
   */
  def knnJoin(rdd : RDD[Vector[String]], data_point : Vector[String], len : Int, 
      random_size : Int) : RDD[Vector[String]] = {
   
   // val sc = new SparkContext("local", "knnJoin_datapoint")
    
    val size = rdd.first.length
    val rand = new Array[Double](size)//mutable.Buffer
   
  
   val hash =  Hashing.murmur3_128(5)
   val model = zScore.computeScore(rdd, hash)
   val data_score = zScore.computeDataScore(data_point, model.mean, model.variance)
   
   for(count <- 0 to size-1) rand(count) = 0.0
   
   val c_i = knnJoin_perIteration(rdd, data_point, rand.toList.map(word => word.toString).toVector ,len,model.score, data_score)
   c_i.persist
   
   
   //remove the first entry from the data-set returned by knnJoin_perIteration 
   var i = -1
   val c_i2 = c_i.filter(word => {i = i+1
     i} > 0)
     
      //println(paral.count + " - " + c_i2.count)
     c_i2.saveAsTextFile("/home/kaushik/Desktop/a1")
     
   //compute - its the rdd where data-set generated from each iteration is being recursively appended 
   var compute = c_i2
   compute.persist
   
   
   //the no.of iterations to be performed
   for(coun <- 2 to random_size) {
     
     for(i <- 0 to size-1) rand(i) = random
     
    		 
     /**
      * ADD CODE TO INCREMENT EACH DATA-POINT OF DATASET BY rand
      * before applying knnJoin_perIteration of that data-set
      */
   
     val modelLooped = zScore.computeScore(rdd, hash)
     val data_scoreLooped = zScore.computeDataScore(data_point, modelLooped.mean, modelLooped.variance)
     
     val c_iLooped = knnJoin_perIteration(rdd, data_point, rand.toList.map(word => word.toString).toVector ,len, modelLooped.score, data_score)
     
     c_iLooped.persist
    
     var iLooped = -1
     var c_i2Looped = c_iLooped.filter(word => {iLooped = iLooped+1
     	iLooped} > 0)
     
     c_i2Looped.saveAsTextFile("/home/kaushik/Desktop/a"+coun)
     compute = compute.union(c_i2Looped)
     //persist of data is required, else abnormal result is being en-countered 
     compute.persist
   }
    
   
     compute.saveAsTextFile("/home/kaushik/Desktop/final")
     compute
   
  }
  
}
