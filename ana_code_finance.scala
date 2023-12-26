import org.apache.spark.sql.{SparkSession, DataFrame, functions}
import org.apache.spark.sql.functions._
import scala.collection.mutable.ListBuffer

// Hard-coded list of Developed Countries
val developedCountries = List("united states", "canada", "united kingdom", "germany", "france", "japan", 
  "australia", "switzerland", "sweden", "norway", "denmark", "netherlands", "finland", "singapore", "south korea",
  "new zealand", "austria", "belgium", "ireland", "luxembourg", "iceland", "italy", "spain", "portugal", "greece",
  "israel", "united arab emirates", "china")

val countryMapping = Map(
    "bolivia (plurinational state of)" -> "bolivia",
    "côte d'ivoire" -> "cote d ivoire",
    "czechia" -> "czech republic",
    "iran (islamic republic of)" -> "iran",
    "united kingdom of great britain and northern ireland" -> "united kingdom",
    "venezuela (bolivarian republic of)" -> "venezuela",
    "türkiye" -> "turkey",
    "micronesia (federated states of)" -> "federated states of micronesia",
    "guinea-bissau" -> "guinea bissau"
  )


def loadAndProfileDataset(spark: SparkSession, path: String): DataFrame = {
  val df = spark.read
    .option("header", "true")   // Use the first line of the CSV file as the header
    .csv(path)

  // Profiling part (OPTIONAL)

  // Print schema and sample data
  println(s"Dataset Schema for $path:")
  df.printSchema()
  println(s"Sample Data for $path:")
  df.show(5, truncate = false)

  // Basic statistical summary
  println(s"Summary Statistics for $path:")
  df.describe().show()

  // Check for null values
  println(s"Null Values in $path:")
  df.columns.foreach { col =>
    val nullCount = df.filter(df(col).isNull || df(col) === "").count()
    println(s"$col: $nullCount null values")
  }

  // Check for distinct values
  println(s"Distinct Values in $path:")
  df.columns.foreach { col =>
    val distinctCount = df.select(col).distinct().count()
    println(s"$col: $distinctCount distinct values")
  }

  df
}

// Function to clean CO2 emissions data
def cleanCO2Dataset(data: DataFrame): DataFrame = {
  // Mapping table for country names
  val countryMapping = Map(
    "france (including monaco)" -> "france",
    "occupied palestinian territory" -> "state of palestine",
    "st. kitts and nevis" -> "saint kitts and nevis",
    "st. vincent & the grenadines" -> "saint vincent and the grenadines",
    "sao tome & principe" -> "sao tome and principe",
    "plurinational state of bolivia" -> "bolivia",
    "democratic people s republic of korea" -> "democratic people's republic of korea",
    "timor-leste (formerly east timor)" -> "timor-leste",
    "myanmar (formerly burma)" -> "myanmar",
    "democratic republic of the congo (formerly zaire)" -> "democratic republic of the congo",
    "republic of cameroon" -> "cameroon",
    "lao people s democratic republic" -> "lao people's democratic republic",
    "czech republic (czechia)" -> "czech republic",
    "islamic republic of iran" -> "iran",
    "italy (including san marino)" -> "italy",
    "bosnia & herzegovina" -> "bosnia and herzegovina",
    "timor-leste (formerly east timor)" -> "timor-leste",
    "libyan arab jamahiriyah" -> "libya",
    "antigua & barbuda" -> "antigua and barbuda"
  )

  // Data cleaning - filter the years to retain only those between 2000 and 2023
  val data_filtered = data.filter(col("Year") >= 2000 && col("Year") <= 2023)

  // Select only the desired columns: Year, Country, Total, Per Capita and drop rows with null values
  val data_selected = data_filtered.select("Year", "Country", "Total", "Per Capita").na.drop()

  // Update country names based on the mapping
  val mapCountryName = udf((country: String) => countryMapping.getOrElse(country.toLowerCase, country))
  val updatedData = data_selected.withColumn("Country", mapCountryName(lower(col("Country"))))

  // Reduce the data to a single partition for output as a single file
  val data_coalesced = updatedData.coalesce(1)

  data_coalesced.show(false)

  data_coalesced
}

def cleanFinanceDataset(df: DataFrame, countryFilter: Option[List[String]] = None, technologyFilter: Option[String] = None): DataFrame = {
  // Remove rows with null values in any column
  val cleanedDF = df.na.drop().filter(col("Year") >= 2000 && col("Year") <= 2023)

  // Convert age column to integer and replace null values with a default value (e.g., 0)
  val outputDF = cleanedDF.withColumn("TotalFinanceFlow", col("`Amount (2020 USD million)`").cast("double"))

  // Update country names based on the mapping
  val mapCountryName = udf((country: String) => countryMapping.getOrElse(country.toLowerCase, country))
  val updatedData = outputDF.withColumn("Country", mapCountryName(lower(col("Country/Area"))))

  // Apply filters only if they are not None
  val filteredDF = (countryFilter, technologyFilter) match {
    case (Some(countries), Some(tech)) =>
      updatedData.filter(col("Country").isin(countries: _*) && col("Technology") === tech)
    case (Some(countries), None) =>
      updatedData.filter(col("Country").isin(countries: _*))
    case (None, Some(tech)) =>
      updatedData.filter(col("Technology") === tech)
    case (None, None) =>
      updatedData
  }

  filteredDF
}


def calculateCorrelation(df: DataFrame, col1: String, col2: String): Double = {
    // Use the corr method to calculate the correlation coefficient
    df.select(round(corr(col1, col2), 3)).as[Double].first()
  }


// Create a Spark session
val spark = SparkSession.builder
  .appName("Big Data Project")
  .getOrCreate()

// Load, profile, and clean the first dataset
val dataset1Path = "hdfs://nyu-dataproc-m/user/ak8827_nyu_edu/project/fossil_co2.csv"
val df1 = loadAndProfileDataset(spark, dataset1Path)
val clean_df1 = cleanCO2Dataset(df1)

// Load, profile, and clean the second dataset
val dataset2Path = "hdfs://nyu-dataproc-m/user/ak8827_nyu_edu/project/investment_data.csv"
val df2 = loadAndProfileDataset(spark, dataset2Path)
val clean_df2 = cleanFinanceDataset(df2)

// //OPTIONAL: Uses profiling functions to determine countries which did not match between two datasets
// //Using this we determined the mappings used in cleaning functions
// val list1 = extractUniqueValues(clean_df1, "Country")
// val list2 = extractUniqueValues(clean_df2, "Country")

// // Find non-matching values
// val nonMatchingValues = findNonMatchingValues(list1, list2)
// // Print the result
// println(s"Non-matching values: $nonMatchingValues")


val summedData = clean_df2.groupBy("Country", "Year").agg(functions.sum("TotalFinanceFlow").alias("TotalAmount"))
summedData.show()

val summed_co2 = clean_df1.groupBy("Country", "Year").agg(round(functions.sum("Total"), 2).alias("TotalCO2Amount"))
summed_co2.show()

// Register the DataFrames as a temporary SQL tables
summed_co2.createOrReplaceTempView("emissions")
summedData.createOrReplaceTempView("investments")
//summed_re.createOrReplaceTempView("renewable_energy")

// Base query to join investment and emissions data
val query = """SELECT i.country as Country, i.year as investment_year, e.year as emissions_year, i.TotalAmount as investment_amount, e.TotalCO2Amount as total_emission
FROM investments as i
JOIN emissions as e ON i.country = e.country 
"""

// Loop to answer the question: How long does it take for an investment 
// to take effect on carbon dioxide emissions? 
for (i <- 0 to 5) {
  val corrQuery = s"$query AND i.year = e.year - $i;"
  val correlationTable: DataFrame = spark.sql(corrQuery)

  // Show the results
  correlationTable.describe().show()

  val column1 = "investment_amount"
  val column2 = "total_emission"

  // Calculate the correlation coefficient between the two columns
  val correlationCoefficient = calculateCorrelation(correlationTable, column1, column2)

  // Print the result
  println(s"Correlation Coefficient between $column1 and $column2: $correlationCoefficient")

  //OPTIONAL: Write resulting correlations into a table for further visualizations
  // val data_coalesced = correlationTable.coalesce(1)
  // // Write the results to a separate table
  // data_coalesced.write.csv(s"project/corr_inv_co2_$i")
}

// Stop the Spark session
spark.stop()
