import org.apache.spark.{SparkConf, SparkContext}
import java.util.Locale

object Top10Words {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Top10Words").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val startTime = System.nanoTime()

    val lines = sc.textFile("tweets_cleaned.csv")
    val header = lines.first()
    val data = lines.filter(line => line != header)

    val columns = header.split(",").map(_.trim)
    val textIndex = columns.indexOf("text")
    val langIndex = columns.indexOf("lang")


    val words = data.flatMap { line =>
        val parts = line.split(",").map(_.trim)
        if (parts(langIndex) == "en") {
          val text = parts(textIndex)
          text.split("\\s+")
        }
        else {
          Array.empty[String]
        }
      }
      .map(word => word.toLowerCase(Locale.ROOT).trim)
      .filter(word => !commonWord(word) && word.length > 4 && !word.contains("#") && !word.contains("corona") && !word.contains("covid"))

    val counts = words.map(tag => (tag, 1)).reduceByKey(_ + _)

    val top10 = counts.sortBy({ case (_, count) => count }, ascending = false).take(10)

    top10.foreach { case (word, count) =>
      println(s"$word: $count")
    }

    val endTime = System.nanoTime()
    println(f"Job completed in ${(endTime - startTime) / 1e9}%.2f seconds")

    sc.stop()
  }

  private val commonWord: Set[String] = Set(
    "a", "about", "above", "after", "again", "against", "all", "am", "an",
    "and", "any", "are", "aren't", "as", "at", "be", "because", "been",
    "before", "being", "below", "between", "both", "but", "by", "can",
    "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does",
    "doesn't", "doing", "don't", "down", "during", "each", "few", "for",
    "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't",
    "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers",
    "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll",
    "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its",
    "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", "no",
    "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought",
    "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she",
    "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such",
    "than", "that", "that's", "the", "their", "theirs", "them", "themselves",
    "then", "there", "there's", "these", "they", "they'd", "they'll", "they're",
    "they've", "this", "those", "through", "to", "too", "under", "until", "up",
    "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were",
    "weren't", "what", "what's", "when", "when's", "where", "where's", "which",
    "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would",
    "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours",
    "yourself", "yourselves", "&amp;", "-", "will", "get", "just"
  )
}
