// Databricks notebook source
//import the library required to stream twitter
//@author Radhika Kalaiselvan
import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import scala.math.Ordering
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import org.apache.spark.streaming.twitter.TwitterUtils

// COMMAND ----------

//Set the twitter API credentials
System.setProperty("twitter4j.oauth.consumerKey", "")
System.setProperty("twitter4j.oauth.consumerSecret","")
System.setProperty("twitter4j.oauth.accessToken", "")
System.setProperty("twitter4j.oauth.accessTokenSecret","")

// COMMAND ----------

// Recompute the top hashtags 5 seconds 
val slideInterval = new Duration(5 * 1000)

// Compute the top hashtags for the 30 seconds
val windowLength = new Duration(30 * 1000)

// Wait this many seconds before stopping the streaming job
val timeoutJobLength = 100 * 1000

// COMMAND ----------


var newContextCreated = false
var batch_num = 0

// COMMAND ----------

// This is a helper class used for ordering based on the count of the tweet
object SecondValueOrdering extends Ordering[(String, Int)] {
  def compare(a: (String, Int), b: (String, Int)) = {
    a._2 compare b._2
  }
}


// COMMAND ----------

// This is the function that creates the SteamingContext and sets up the Spark Streaming job.
def creatingFunc(): StreamingContext = {
  // Create a Spark Streaming Context of sliding interval 30 seconds
  val ssc = new StreamingContext(sc, slideInterval)
  
  // Create a Twitter Stream for the input source. 
  val auth = Some(new OAuthAuthorization(new ConfigurationBuilder().build()))
  val twitterStream = TwitterUtils.createStream(ssc, auth)
  
  // Parse the tweets and gather the hashTags.
  val hashTagStream = twitterStream.map(_.getText).flatMap(_.split(" ")).filter(_.startsWith("#"))
  
  // Compute the counts of each hashtag by window.
  val windowedhashTagCountStream = hashTagStream.map((_, 1)).reduceByKeyAndWindow((x: Int, y: Int) => x + y, windowLength, slideInterval)

  // For each window, calculate the top hashtags for that time period.
  windowedhashTagCountStream.foreachRDD(hashTagCountRDD => {
    //Gives the top 10 tweets with max count      
    val topEndpoints = hashTagCountRDD.top(10)(SecondValueOrdering)
    println(s"------ TOP HASHTAGS For window ${batch_num}")
    println(topEndpoints.mkString("\n"))
    batch_num = batch_num + 1
    if(batch_num > 10){
      //Not the right way to exit a program, change later
      System.exit(0)
    }
  })
  
  newContextCreated = true
  ssc
}


// COMMAND ----------

@transient val ssc = StreamingContext.getActiveOrCreate(creatingFunc)

// COMMAND ----------

ssc.start()
ssc.awaitTerminationOrTimeout(timeoutJobLength)

// COMMAND ----------


