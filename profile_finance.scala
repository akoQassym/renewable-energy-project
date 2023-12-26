import org.apache.spark.sql.{SparkSession, DataFrame, functions}
import org.apache.spark.sql.functions._

// Data loading. Create DataFrame from CSV file
def loadDataset(spark: SparkSession, path: String): DataFrame = {
  val df = spark.read
    .option("header", "true")   // Use the first line of the CSV file as the header
    .csv(path)
  df
}


//Code used to determine which countries do not match between two diff datasets
def extractUniqueValues(df: DataFrame, columnName: String): List[String] = {
    val uniqueValuesBuffer = ListBuffer[String]()

    // Collect unique values using Spark's distinct() method
    val uniqueValuesDF = df.select(columnName).distinct()

    // Convert DataFrame to a list of strings
    uniqueValuesDF.collect().foreach { row =>
      val value = row.getAs[String](columnName)
      uniqueValuesBuffer += value
    }

    uniqueValuesBuffer.toList
  }

def findNonMatchingValues(list1: List[String], list2: List[String]): List[String] = {
    // Use Set operations to find non-matching values
    val set1 = list1.toSet
    val set2 = list2.toSet

    val nonMatchingSet = (set1 diff set2)

    nonMatchingSet.toList
  }


//Data Profiling function. Prints out Schema, Statistical Summary, N of null and N of distinct values
def profileDataset(spark: SparkSession, df: DataFrame): DataFrame = {

  // Print schema and sample data
  println(s"Dataset Schema for RE Finance Flows:")
  df.printSchema()
  println(s"Sample Data for RE Finance Flows:")
  df.show(25, truncate = false)

  // Basic statistical summary
  println(s"Summary Statistics for RE Finance Flows:")
  df.describe().show()

  // Check for null values
  println(s"Null Values in RE Finance Flows:")
  df.columns.foreach { col =>
    val nullCount = df.filter(df(col).isNull || df(col) === "").count()
    println(s"$col: $nullCount null values")
  }

  // Check for distinct values
  println(s"Distinct Values in RE Finance Flows:")
  df.columns.foreach { col =>
    val distinctCount = df.select(col).distinct().count()
    println(s"$col: $distinctCount distinct values")
  }
}

val spark = SparkSession.builder
  .appName("Project Profiling")
  .getOrCreate()


// Load and profile the RE investment dataset
val datasetPath = "project/investment_data.csv"
val df = loadDataset(spark, datasetPath)
profileDataset(spark, df)

spark.stop()