import org.apache.spark.{SparkConf, SparkContext}
import java.util.Locale

object Top10Hashtags {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Top10Hashtags").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val startTime = System.nanoTime()

    val lines = sc.textFile("tweets_cleaned.csv")
    val header = lines.first()
    val data = lines.filter(line => line != header)

    val columns = header.split(",").map(_.trim)
    val textIndex = columns.indexOf("text")
    if (textIndex == -1) {
      println("No 'text' column found")
      sys.exit(1)
    }

    val hashtags = data.flatMap { line =>
        val parts = line.split(",").map(_.trim)
        val text = parts(textIndex)
        text.split("\\s+")
      }
      .filter(word => word.startsWith("#"))
      .map(word => word.toLowerCase(Locale.ROOT).trim)
      .filter(word => !(word.contains("covid") || word.contains("corona")))

    val counts = hashtags.map(tag => (tag, 1)).reduceByKey(_ + _)

    val top10 = counts.sortBy({ case (_, count) => count }, ascending = false).take(10)

    top10.foreach { case (tag, count) =>
      println(s"$tag: $count")
    }

    val endTime = System.nanoTime()
    println(f"Job completed in ${(endTime - startTime) / 1e9}%.2f seconds")

    sc.stop()
  }
}
