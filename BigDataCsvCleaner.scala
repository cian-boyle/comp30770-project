import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.{lower, regexp_replace, col}

object BigDataCsvCleaner {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder()
      .appName("CSV Data Cleaner")
      .master("local[*]")
      .getOrCreate()

    val inputPath = "combined.csv"
    val outputPath = "tweets_cleaned"

    val df: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiLine", "true")
      .csv(inputPath)


    val cleanedDf = df.withColumn("text",
      regexp_replace(
        regexp_replace(lower(col("text")), "[.,]", ""),
        "[\\r\\n]+", " "
      )
    )

    cleanedDf.write
      .option("header", "true")
      .csv(outputPath)

    spark.stop()
  }
}
