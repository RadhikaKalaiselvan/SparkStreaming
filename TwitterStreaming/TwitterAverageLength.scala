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

// Recompute the top hashtags 5 seconds 
val slideInterval = new Duration(5 * 1000)

// Compute the top hashtags for the 30 seconds
val windowLength = new Duration(30 * 1000)


var newContextCreated = false
var batch_num = 0

// COMMAND ----------

import java.util.concurrent.atomic.AtomicLong
//Since multiple threads will be updating these counter values we will use atomiclong to make it thread safe
var totalTweets=new AtomicLong(0)
var totalChars=new AtomicLong(0)

// COMMAND ----------

// This is the function that creates the SteamingContext and sets up the Spark Streaming job.
def creatingFunc(): StreamingContext = {
  // Create a Spark Streaming Context of sliding interval 30 seconds
  val ssc = new StreamingContext(sc, slideInterval)
  
  // Create a Twitter Stream for the input source. 
  val auth = Some(new OAuthAuthorization(new ConfigurationBuilder().build()))
  val twitterStream = TwitterUtils.createStream(ssc, auth)
  
  // Parse the tweets and gather the hashTags.
  val hashTagStream = twitterStream.map(_.getText)
  val lengths=hashTagStream.map(x => x.length())
  // For each window, calculate the avg length
  lengths.foreachRDD(hashTagRDD => {
   var count = hashTagRDD.count()
      if (count > 0) {
        totalTweets.getAndAdd(count)
        
        totalChars.getAndAdd(hashTagRDD.reduce((x,y) => x + y))
        
        println("Total tweets: " + totalTweets.get() + 
            " Total characters: " + totalChars.get() + 
            " Average: " + totalChars.get() / totalTweets.get())
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


