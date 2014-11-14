/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/**
 * Get z-scores of all data-points in the dataset
 * Get z-score of the data-point being knn-joined against the dataset
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Vector
import scala.math._
import com.google.common.hash.HashFunction

class zScoreModel (
  val score : RDD[(Long,Double)],
  val mean : RDD[Double],
  val variance : RDD[Double] 
)
/**
 * Top-level methods for zScore.
 */
object zScore {
  
 
  /**
   * Computes the mean (u1 , u2, ... uk) for k-dimensional data
   * 
   * @param rdd : RDD of Vectors of String
   * @return a RDD of mean values for each dimension
   */
  private def computeMean(rdd : RDD[Vector[String]]): RDD[Double] = {
   
    
    //dimension of data point
    val attributeLength = rdd.first.length
    //no.of data points
    val size = rdd.count.toDouble
    
    var i = -1L
    val mean = rdd.flatMap(line => line.seq).
    				map(word => ({
    				  i = i+1
    				  i
    				}%attributeLength) -> word.toDouble ).
    				groupByKey.
    				map(word => word._1 -> ((word._2.foldLeft(0.0)(_+_))/size)).
    				sortByKey(true).
    				map(word=> word._2)
    				
    				
//    				
    mean
    
  }
  
  /**
   * Computes the variance (v1 , v2, ... vk) for k-dimensional data
   * 
   * @param rdd : RDD of Vectors of String
   * @param mean : RDD of mean values for each dimension
   * @return a RDD of variance values for each dimension
   */
  private def computeVariance(rdd : RDD[Vector[String]], 
      mean : RDD[Double])
  : RDD[Double] = {
    
    //dimension of data point
    val attributeLength = rdd.first.length
    //no.of data points
    val size = rdd.count
    
    
    val mean_temp = mean.collect
    
    var i = -1L
    val variance = rdd.flatMap(line => line.seq).
    				map(word => ({
    				  i = i+1
    				  i
    				}%attributeLength) -> pow((word.toDouble - mean_temp(i.toInt%attributeLength)),2)).
    				groupByKey.
    				map(word => word._1 -> ((word._2.foldLeft(0.0)(_+_))/size)).
    				sortByKey(true).map(word => word._2)
   variance
  }
  
  
  /**
   * Computes the z-score for each data-point within the dataset
   * 
   * @param rdd : RDD of Vectors of String
   * @param mean : RDD of mean values for each dimension
   * @param mean : RDD of variance values for each dimension
   * @return zScoreModel, which comprises of 	(1)RDD of z-scores for each data-point
   * 											(2)mean and variance of the data-set on which KNN join is applied
   */
   
  private def compute(rdd : RDD[Vector[String]], 
      mean : RDD[Double], 
      variance : RDD[Double])
  : zScoreModel = {

    
    //dimension of data point
    val attributeLength = rdd.first.length
    val size = rdd.count + 1
    val mean_temp = mean.collect
    val variance_temp = variance.collect
    
    var i = -1L
    val score = rdd.zipWithIndex.map(line => line._1.map(word => line._2 -> (pow((word.toDouble - mean_temp(({i = i+1
      					i%attributeLength}).toInt)),2 )/ variance_temp((i%attributeLength).toInt) )) ).
      					flatMap(line => line.seq).
      					groupByKey.
      					map(word =>  sqrt(word._2.foldLeft(0.0)(_+_)) -> word._1).
      					sortByKey(true).map(word => word._2 -> word._1)

    			
    /**
     * check validity of score
     * once back home!!!
     */
 
   	new zScoreModel(score,mean,variance)
    
  }

    
  /**
   * This function acts as an entry point to compute the z-scores of the data-set
   *
   * @param rdd : RDD of Vectors of String 
   * 
   * @return zScoreModel, which comprises of 	(1)RDD of z-scores for each data-point
   * 											(2)mean and variance of the data-set on which KNN join is applied
   */    
  def computeScore(rdd : RDD[Vector[String]])
  : zScoreModel = {

    val mean = computeMean(rdd)
    val variance = computeVariance(rdd, mean)
    val score = compute(rdd, mean, variance)
    
    score
  }
  
  
  
  /**
   * This function computes the score of a single data-point against the data-set 
   * it is being compared against
   *
   * @param rdd : RDD of Vectors of String
   * @param mean : RDD of Double
   * @param variance : RDD of Double 
   * @return z-score for the data-point being compared against the data-set
   */  
  def computeDataScore(data : Vector[String], mean : RDD[Double], variance : RDD[Double]) : Double = {
    val mean_temp = mean.collect
    val variance_temp = variance.collect
    val attributeLength = data.length
    
    var i = -1L
    val data_score = data.map(word => (pow((word.toDouble - mean_temp({
      i = i+1
      i
    }.toInt%attributeLength)),2 )/ variance_temp(i.toInt%attributeLength) ) ).
    			foldLeft(0.0)(_+_)
    sqrt(data_score)
    
  }
   
}
