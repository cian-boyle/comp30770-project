import org.apache.spark.{SparkConf, SparkContext}
import java.util.Locale

object TweetSentimentAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TweetSentimentAnalysis").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val startTime = System.nanoTime()

    val lines = sc.textFile("tweets_cleaned.csv")
    val header = lines.first()
    val data = lines.filter(line => line != header)

    val columns = header.split(",").map(_.trim)
    val textIndex = columns.indexOf("text")

    val positiveKeywords = Set(
      "getting better", "recovered", "recovering", "improving", "better",
      "stable", "optimistic", "hopeful", "hope", "hoping", "inspiring",
      "encouraging", "bright", "optimism", "promising", "upbeat"
    )

    val negativeKeywords = Set(
      "getting worse", "worse", "death", "deaths", "died", "fatal",
      "hospitalized", "infection", "outbreak", "lockdown", "quarantine",
      "suffering", "loss", "grief"
    )

    val sentimentCounts = data.map { line =>
      val parts = line.split(",").map(_.trim)
        val text = parts(textIndex)
        val isPositive = positiveKeywords.exists(keyword => text.contains(keyword))
        val isNegative = negativeKeywords.exists(keyword => text.contains(keyword))

        (if (isPositive) 1 else 0, if (isNegative) 1 else 0)
    }
      .reduce((a, b) => (a._1 + b._1, a._2 + b._2))

    println(s"Positive tweets: ${sentimentCounts._1}")
    println(s"Negative tweets: ${sentimentCounts._2}")

    val endTime = System.nanoTime()
    println(f"Job completed in ${(endTime - startTime) / 1e9}%.2f seconds")

    sc.stop()
  }
}