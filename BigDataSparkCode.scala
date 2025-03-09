import org.apache.spark.{SparkConf, SparkContext}
import java.util.Locale

object Top10Hashtags {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Top10Hashtags").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val startTime = System.nanoTime()

    // Read the CSV file as text; the first line is assumed to be the header.
    val lines = sc.textFile("/Users/cianboyle/Desktop/Random Code/scalapractice/tweets_uncleaned.csv")
    val header = lines.first()
    val data = lines.filter(line => line != header)

    // Parse header to find the index of the "text" column.
    val columns = header.split(",").map(_.trim)
    val textIndex = columns.indexOf("text")
    if (textIndex == -1) {
      println("No 'text' column found")
      sys.exit(1)
    }

    // For each line, split the CSV (a simple comma split) and then split the text column by whitespace.
    val hashtags = data.flatMap { line =>
        val parts = line.split(",").map(_.trim)
        if (parts.length > textIndex) {
          val text = parts(textIndex)
          text.split("\\s+")
        } else {
          Array.empty[String]
        }
      }
      .filter(word => word.startsWith("#"))
      .map(word => word.toLowerCase(Locale.ROOT).trim)
      .filter(word => !(word.contains("covid") || word.contains("corona")))

    // Count occurrences of each hashtag.
    val counts = hashtags.map(tag => (tag, 1)).reduceByKey(_ + _)

    // Get the top 10 hashtags (sorted descending by count).
    val top10 = counts.sortBy({ case (_, count) => count }, ascending = false).take(10)

    // Print the results.
    top10.foreach { case (tag, count) =>
      println(s"$tag: $count")
    }

    val endTime = System.nanoTime()
    println(f"Job completed in ${(endTime - startTime) / 1e9}%.2f seconds")

    sc.stop()
  }
}

import org.apache.spark.{SparkConf, SparkContext}

object TweetSentimentCount {
  def main(args: Array[String]): Unit = {
    // Set up Spark configuration and context.
    val conf = new SparkConf().setAppName("TweetSentimentCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Record start time
    val startTime = System.nanoTime()

    // Path to the CSV file.
    val filePath = "tweets_uncleaned.csv"  // adjust the path if necessary

    // Read the CSV file as text.
    val lines = sc.textFile(filePath)

    // Assume the first line is a header.
    val header = lines.first()
    val data = lines.filter(line => line != header)

    // Split header to find the index for the "text" column.
    // (This simple split assumes the CSV is well-behaved.)
    val headerColumns = header.split(",")
    val textIndex = headerColumns.indexOf("text")

    // Parse each line to extract the tweet text and convert it to lowercase.
    val tweets = data.map(line => line.split(",", -1))
      .filter(fields => fields.length > textIndex)
      .map(fields => fields(textIndex).toLowerCase)

    // Define functions to detect positive and negative sentiment.
    def isPositive(tweet: String): Boolean = {
      tweet.contains("happy") ||
        tweet.contains("joy") ||
        tweet.contains("delight") ||
        tweet.contains("hope") ||
        tweet.contains("getting better") ||
        tweet.contains("recover")
    }

    def isNegative(tweet: String): Boolean = {
      tweet.contains("sad") ||
        tweet.contains("angry") ||
        tweet.contains("depress") ||
        tweet.contains("bad") ||
        tweet.contains("death") ||
        tweet.contains("die") ||
        tweet.contains("dead") ||
        tweet.contains("sick")
    }

    // Filter the tweets RDD into two RDDs with sentiment labels.
    val positiveRDD = tweets.filter(isPositive).map(_ => ("positive", 1))
    val negativeRDD = tweets.filter(isNegative).map(_ => ("negative", 1))

    // Combine both RDDs and count tweets per sentiment.
    val sentimentCounts = positiveRDD.union(negativeRDD)
      .reduceByKey(_ + _)

    // Print the results.
    sentimentCounts.collect().foreach { case (sentiment, count) =>
      println(s"$sentiment: $count")
    }

    // Stop Spark context.
    sc.stop()

    // Report job execution time.
    val endTime = System.nanoTime()
    val durationSeconds = (endTime - startTime) / 1e9
    println(f"Job completed in $durationSeconds%.2f seconds")
  }
}