package com.twitter.spark.streaming

// Only needed for Spark Streaming.
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import com.google.gson.Gson

import java.io.FileWriter

// Only needed for utilities for streaming from Twitter.
import org.apache.spark.streaming.twitter._


object CollectingTweets {
  
  val resourcePath = "src/main/resources/output/"
  val numTweetsToCollect = 10 
	var tweetsCollected:Int = 0
	val gson = new Gson()
  
  
   def main(args: Array[String]){ 
      //Variables that contains the user credentials to access Twitter API 
    val accessToken = "41644104-e7ObeMxiEOAkZ3PUu7cil2PhnglgV4wLjRKFuUVML"
    val accessTokenSecret = "sQvRCQVlnQ3jbR9ddVGJnRjGrgvvXGmz5fxjLsqbyZT3u"
    val consumerKey = "BI6IVrrX9S3PB0lBtnju9Dq1G"
    val consumerSecret = "5QBDVOzRZ2IDjxtxLoyl0dPxoq2xqdB0oDcpHHM1gxus1Wb0oS" 
  
    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
    
     //Create a SparkConf object 
    val sparkConf = new SparkConf().setAppName("CollectingTweets").setMaster("local[8]")
    val sc = new SparkContext(sparkConf)	
    
    val fw = new FileWriter(resourcePath + "tweets_collected_apa.json", true)
    // Use the config to create a streaming context that creates a new RDD
    // with a batch interval of every 5 seconds.
    val ssc = new StreamingContext(sc, Seconds(20))
    
     // Use the streaming context and the TwitterUtils to create the Twitter stream.
    // 
    /**Input parameters:
     * ssc = spark streaming context
     * twitter4j auth
     * filter
     */
    val streamTweets = TwitterUtils.createStream(ssc, None,Seq("President Trump", "Elizabeth Warren"))
    
    
    //val tweetsWithTimeZone = streamTweets.filter(status=>(status.getPlace() != null))
    
    //val swedishTweets = tweetsWithTimeZone.filter(status=>status.getPlace().getCountry().contains("Sweden"))
    //parse json tweets with using GSON library
    val tweets = streamTweets.map(gson.toJson(_))
    
    tweets.print()
		//collect and save tweets in the file
		tweets.foreachRDD(rdd => {
		  
			if(!rdd.isEmpty()){
				val result = rdd.collect

				for(tweet <- result){
					fw.write(tweet + "\n")

					tweetsCollected += 1
					
					if(tweetsCollected >= numTweetsToCollect){
						println("---------- COLLECTED TWEETS: " + tweetsCollected + " ----------")
						
						fw.close()
						
						sys.ShutdownHookThread {
              ssc.stop(true, true)
              println("Application stopped")
            }
					}		
				}
			}
		})
   
   def inSweden(timezone:String):Boolean = {
      if (timezone != null){
        if(timezone.contains("Sweden")){
          return true
        }
        else {return false}
      }
      else {return false} 
   }
    
    
    
   ssc.start()
   ssc.awaitTermination() 
    
   }
  
}