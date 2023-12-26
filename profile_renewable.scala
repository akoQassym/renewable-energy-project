import org.apache.spark.sql.{SparkSession, DataFrame, functions}
import org.apache.spark.sql.functions._


// Data Loading and Profiling function. Prints out Schema, Statistical Summary, N of null and N of distinct values
def loadAndProfileDataset(spark: SparkSession, path: String): DataFrame = {
  val df = spark.read
    .option("header", "true")   // Use the first line of the CSV file as the header
    .csv(path)
    .withColumnRenamed("Avg. HH size", "AvgHHSize")

  // Print schema and sample data
  println(s"Dataset Schema for RE Generation dataset:")
  df.printSchema()
  println(s"Sample Data for RE Generation dataset:")
  df.show(10, truncate = false)

  // Basic statistical summary
  println(s"Summary Statistics for RE Generation dataset:")
  df.describe().show()

  // Check for null values
  println(s"Null Values in RE Generation dataset:")
  df.columns.foreach { col =>
    val nullCount = df.filter(df(col).isNull || df(col) === "").count()
    println(s"$col: $nullCount null values")
  }

  // Check for distinct values
  println(s"Distinct Values in RE Generation dataset:")
  df.columns.foreach { col =>
    val distinctCount = df.select(col).distinct().count()
    println(s"$col: $distinctCount distinct values")
  }

  df
}

val spark = SparkSession.builder
  .appName("Project Profiling")
  .getOrCreate()

// Load and profile the RE investment dataset
val datasetPath = "project/renewable_data.csv"
val df = loadAndProfileDataset(spark, datasetPath)


spark.stop()