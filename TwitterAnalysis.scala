package com.twitter.spark.sparksql

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

//Mllib
import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer,HashingTF,IDF}
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.Word2Vec
//models
import org.apache.spark.ml.clustering.KMeans


/***
 * Spark SQL
 * Load the json data 
 * Understanding the data
 * Cleaning the data
 *     - Dealing with missing, incomplete data
 *     - Filtering data
 * Feature extraction 
 */

object TwitterAnalysis {
  
  def main(args: Array[String]) {
    
    //Create SQL  Context in local mode
    val sparkConf = new SparkConf().setAppName("TwitterAnalysis").setMaster("local[8]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    
      //to convert $'col name' into an Column 
    import sqlContext.implicits._
    
    //Load tweets as Dataframe to Spark
    val tweetsDF = sqlContext.read.json("C:\\Users\\torbj\\Downloads\\LabSessionI\\src\\main\\resources\\output\\tweets.json")
    
    //Print the data structure 
    //tweetsDF.printSchema()
    //println(tweetsDF.count())

    
    /*Understanding the data*/
     
    //User Location Counting 
   println("---------------------User Location Counting --------------------------------")
    //Get top 20 places 
    //tweetsDF.select("place.country").groupBy("country").count().sort($"count".desc).show(20)

    //Get top 20 user locations
    tweetsDF.select("user.location").groupBy("location").count().sort($"count".desc).show(100)
    
    
    //Language Counting 
    println("---------------------User's Languages Counting --------------------------------")
    //Get tweet languages
    tweetsDF.select("lang").groupBy("lang").count().sort($"count".desc).show(100)
    //Get Swedish tweets
    tweetsDF.select("text","lang").where("lang=='en'").show(10,false)
    
    //Visualization in Zepplin ...
    
    /*println("---------------------Basic Analysis --------------------------------")
    //What is trending right now? top 20 popular hashtags
    tweetsDF.select("hashtags").show()
    tweetsDF.select(explode(col("hashtags.text")).as("hashtags_text"))
            .groupBy("hashtags_text")
            .count().sort($"count".desc)
            .show(20)
    */
    //Devices counting 
    //what kind of sources have been used?
   tweetsDF.select("source").groupBy("source").count().sort($"count".desc).show(20,false)
    //What 15 top popular devices/clients have been used 
    val tweetsWithSource = tweetsDF.select("source").where($"source".isNotNull) 
    tweetsWithSource.show()
    tweetsWithSource.select(expr("(split((split(source, '>'))[1],'<'))[0]").cast("string").as("sourceName")).groupBy("sourceName").count().sort($"count".desc).show(15, false)
   
    //What are popular topics for iPhone users?
    /*val iphoneTweets = tweetsDF.filter($"source".contains("Twitter for iPhone"))
    iphoneTweets.select(explode(col("hashtags.text")).as("hashtags_text")).groupBy("hashtags_text").count().sort($"count".desc).show(20)*/
   
   //How many users?    
  tweetsDF.select("user.id").groupBy("id").count().sort($"count".desc).show(100)
   //get unique users 
   println("Total tweets: " + tweetsDF.select("id").count())
   println("Unique users: " + tweetsDF.select("id").distinct().count())
    
   //tweets with emotions 
   //tweetsDF.filter($"text".contains("😍")).show(10,false)
    
    /*Cleaning the data*/ 
   
    println("---------------------Filter tweets by language --------------------------------")
    //Filter tweets by language
    val englishTweets = tweetsDF.filter("user.lang == 'en'")
    englishTweets.select("text").show(20)
    
    println("---------------------Filter tweets by device/source --------------------------------")
    //Filter out records with bad or missing values
    val tweetsWithAllSources = englishTweets.filter($"source".isNotNull) 
    //Filter tweets by device
    val sources = Array("Twitter for iPhone","Twitter for Android","Twitter Web Client","Twitter for iPad")
    val tweetsWithoutBot = tweetsWithAllSources.filter(col("source").contains(sources(0)) || col("source").contains(sources(1)) || col("source").contains(sources(2)) || col("source").contains(sources(3)))
    tweetsWithoutBot.select(expr("(split((split(source, '>'))[1],'<'))[0]").cast("string").as("sourceName")).groupBy("sourceName").count().sort($"count".desc).show(15, false)
    
    println("---------------------Feature extraction --------------------------------")
    //Create new columns based on existing ones
    //For Example:
    val extractSourceName = udf((source: String) => source.split(">")(1).split("<")(0))
    val tweetsWithoutBotWithSource = tweetsWithoutBot.withColumn("sourceName", extractSourceName(col("source")))
    //Select interested data
    //For Example
    val data = tweetsWithoutBotWithSource.select(col("user.createdAt"),col("sourceName"),col("user.id"),col("user.followersCount"),col("lang"),col("text"))
    data.show(20)
    
    println("---------------------Dealing with missing values --------------------------------")
    //Fill in bad or missing data by replacing null values in dataframe.
    //For example:
    val cleanedData = data.na.fill("Other",Seq("timeZone"))
    cleanedData.show(20)
    tweetsDF.select(col("user.createdAt"),col("user.id"),col("user.followersCount"),col("lang"),col("text")).coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("src/main/resources/output/results/")
    tweetsDF.coalesce(1).write.json("src/main/resources/output/result")
    //println("---------------------TEXT FETURES --------------------------------")
    
    //Tokanization 
    //Tokenization is the process of taking text (such as a sentence) and breaking it into individual terms (usually words).
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")   
    val tokenized = tokenizer.transform(tweetsDF)
    tokenized.select("words", "text").take(3).foreach(println)
    
    //Stop Words Removal
    //Stop words are words which should be excluded from the input, typically because the words appear frequently and don’t carry as much meaning.
    //Default stop words for some languages are accessible by calling StopWordsRemover.loadDefaultStopWords(language), for which available options are “danish”, “dutch”, “english”, “finnish”, “french”, “german”, “hungarian”, “italian”, “norwegian”, “portuguese”, “russian”, “spanish”, “swedish” and “turkish”. A boolean parameter caseSensitive indicates if the matches should be case sensitive (false by default).
    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered_words")  
    val stopWordsRemoved = remover.transform(tokenized)
    stopWordsRemoved.select("text","words","filtered_words").take(3).foreach(println)
   
    println("---------------------Data Representation--------------------------------")

   //The Word2VecModel transforms each document into a vector using the average of all words in the document; this vector can then be used as features for prediction, document similarity calculations
    val word2Vec = new Word2Vec().setInputCol("filtered_words").setOutputCol("features").setVectorSize(5).setMinCount(0)
    val model = word2Vec.fit(stopWordsRemoved)
    val preparedData = model.transform(stopWordsRemoved)
    preparedData.select("text","features").take(3).foreach(println)
    
    println("---------------------Tweets Clustering--------------------------------")
    //k-means clsutering
    //train the model
    // Split the data into training and test sets (30% held out for testing)
    val Array(trainingData, testData) = preparedData.randomSplit(Array(0.7, 0.3), seed = 1234L)
    // Trains a k-means model.
    val kmeans = new KMeans().setK(20).setSeed(1L)
    val kmeansModel = kmeans.fit(trainingData.cache())

//    Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = kmeansModel.computeCost(preparedData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    kmeansModel.clusterCenters.foreach(println)
    
     // Select example rows to display.
    val kmeans_predictions = kmeansModel.transform(testData.cache())
    kmeans_predictions.show()
    
    //What are the biggest clusters? In this example:0,7,17
    kmeans_predictions.select("prediction").groupBy("prediction").count().sort($"count".desc).show(20)
    
    //save results in json file
      kmeans_predictions.coalesce(1).write.json("src/main/resources/output/result_kmeans")
//    //Visualizaiton in Zepplin
      

     
   
    
  }
}