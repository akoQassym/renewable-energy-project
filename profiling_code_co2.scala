// Import necessary Spark SQL libraries and functions
import org.apache.spark.sql.{SparkSession, DataFrame, functions}
import org.apache.spark.sql.functions._

// Function to load CO2 emissions dataset into a DataFrame using SparkSession
def loadCO2Dataset(spark: SparkSession, path: String): DataFrame = {
  // Read the dataset from the specified path in CSV format
  val data = spark.read
    .option("header", "true")   // Use the first line of the CSV file as the header
    .csv(path)

  // Return the loaded DataFrame
  data
}

// Function to profile CO2 emissions dataset
def profileCO2Dataset(data: DataFrame, path: String): DataFrame = {
  // Print schema and sample data
  println(s"Dataset Schema for $path:")
  data.printSchema()
  println(s"Sample Data for $path:")
  data.show(5, truncate = false)

  // Basic statistical summary
  println(s"Summary Statistics for $path:")
  data.describe().show()

  // Check for null values in each column
  println(s"Null Values in $path:")
  data.columns.foreach { col =>
    val nullCount = if (col.contains(" ")) {
      val escapedColName = s"`$col`" // Escaping column name with backticks
      data.filter(data(escapedColName).isNull || data(escapedColName) === "").count()
    } else {
      data.filter(data(col).isNull || data(col) === "").count()
    }
    println(s"$col: $nullCount null values")
  }

  // Check for distinct values in each column
  println(s"Distinct Values in $path:")
  data.columns.foreach { col =>
    val distinctCount = if (col.contains(" ")) {
      val escapedColName = s"`$col`" // Escaping column name with backticks
      data.select(escapedColName).distinct().count()
    } else {
      data.select(col).distinct().count()
    }
    println(s"$col: $distinctCount distinct values")
  }

  // Return the DataFrame
  data
}

// Create a SparkSession with the given application name ("Data Profiling")
val spark = SparkSession.builder()
  .appName("Data Profiling")
  .getOrCreate()

// Define the path to the CO2 emissions dataset stored in HDFS
val datasetPath = "hdfs://nyu-dataproc-m/user/ak8827_nyu_edu/project/fossil_co2.csv" 

// Load the CO2 dataset using 'loadCO2Dataset' function and SparkSession
val data = loadCO2Dataset(spark, datasetPath)

// Profile the loaded CO2 data using 'profileCO2Dataset' function
profileCO2Dataset(data, datasetPath)

// Stop the SparkSession to release resources after data processing is complete
spark.stop()
