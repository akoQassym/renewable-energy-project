// Import necessary Spark SQL libraries and functions
import org.apache.spark.sql.{SparkSession, DataFrame, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler

// Function to load CO2 emissions dataset into a DataFrame using SparkSession
def loadDataset(spark: SparkSession, path: String): DataFrame = {
  // Read the dataset from the specified path in CSV format
  val data = spark.read
    .option("header", "true")   // Use the first line of the CSV file as the header
    .csv(path)

  // Return the loaded DataFrame
  data
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

  // Count the number of records after cleaning and display the count
  val count_data_coalesced = data_coalesced.count()
  println(s"Number of records after the cleaning process: $count_data_coalesced")

  // Write the cleaned data to HDFS in a specified path
  data_coalesced.write.mode("overwrite").csv("hdfs://nyu-dataproc-m/user/ak8827_nyu_edu/project/clean_data")

  // Return the cleaned DataFrame
  data_coalesced
}

// Integrated cleaning function for the Renewable Energy generation data with unit standardization
def cleanREDataset(df: DataFrame): DataFrame = {
  // Trim column names to remove any leading or trailing whitespace
  val trimmedDF = df.columns.foldLeft(df)((currentDF, colName) => 
    currentDF.withColumnRenamed(colName, colName.trim)
  )

  val filteredDF = trimmedDF.select("Years", "IRENA Menu Label", "Technology", "Type", "UN Region English", "Unit", "Values")
    //Using universal naming of columns across datasets
    .withColumnRenamed("IRENA Menu Label", "Country")
    .withColumn("Country", lower(col("Country")))
    .withColumnRenamed("Years", "Year")
    // Standardize units: Convert MW to GWh if necessary because we cannot take the "Values" column record without standardization
    .withColumn("Values", 
      when(col("Unit") === "GWh", col("Values"))
      // if the unit is MW it would be divided by 1000 to get GWh value
      .when(col("Unit") === "MW", col("Values") / lit(1000))
      .otherwise(col("Values")))
    .withColumn("Values", col("Values").cast("double"))
    .na.drop()
    .filter(col("Year").between(2000, 2023))

  // Mapping table for country names
  val countryMapping = Map(
    "bolivia (plurinational state of)" -> "bolivia",
    "cote d'ivoire" -> "cote d ivoire",
    "czechia" -> "czech republic",
    "iran (islamic republic of)" -> "iran",
    "north macedonia" -> "macedonia",
    "venezuela (bolivarian republic of)" -> "venezuela"
  )

  // Update country names based on the mapping
  val mapCountryName = udf((country: String) => countryMapping.getOrElse(country.toLowerCase, country))
  val updatedData = filteredDF.withColumn("Country", mapCountryName(lower(col("Country"))))

  updatedData
}

def performAnalytics(co2_data: DataFrame, re_data: DataFrame): Unit = {
  // ANALYSIS 1

  // Convert "Total" column to a numeric type
  val dataWithNumericTotal = co2_data.withColumn("Total", col("Total").cast("double"))
  // Top 10 countries contributing to CO2 emissions
  println("Top 10 countries contributing to CO2 emissions:")
  val top10Contributors = dataWithNumericTotal.groupBy("Country").sum("Total").orderBy(desc("sum(Total)")).limit(10)
  top10Contributors.show(false)

  // ANALYSIS 2

  // Top 10 lowest contributing countries 
  println("Top 10 lowest contributing countries:")
  val low10Contributors = dataWithNumericTotal.groupBy("Country").sum("Total").orderBy(asc("sum(Total)")).limit(10)
  low10Contributors.show(false)

  // ANALYSIS 3

  // Calculate cumulative CO2 emissions for each year across all countries
  val cumulativeEmissions = dataWithNumericTotal.groupBy("Year").agg(round(sum("Total"), 2).alias("CumulativeCO2"))
    .orderBy("Year")
  println("Cumulative CO2 emissions for each year across all countries:")
  cumulativeEmissions.show(false)

  // ANALYSIS 4 (LINEAR REGRESSION)

  // Linear regression analysis for each country
  println("Top 10 countries with the highest increase in contribution (2000-2017):")
  // Convert the 'Year' column to a numeric type for analysis
  val dataWithNumericYear = dataWithNumericTotal.withColumn("Year", col("Year").cast("double"))
  // Aggregate the 'Total' emissions for each 'Year' and 'Country'
  val contributions = dataWithNumericYear.select("Year", "Country", "Total")
    .groupBy("Year", "Country")
    .agg(sum("Total").alias("Total"))
  // Prepare data for linear regression by assembling features
  val assembler = new VectorAssembler()
    .setInputCols(Array("Year"))
    .setOutputCol("features")
  val assembledData = assembler.transform(contributions)
  // Retrieve distinct country names from the assembled data
  val countries = assembledData.select("Country").distinct().collect()
  // Calculate the slope (rate of increase) of emissions for each country using Linear Regression
  val countrySlopes = countries.map { country =>
    val countryName = country.getString(0)
    // Filter data for the current country
    val countryData = assembledData.filter(col("Country") === countryName)
    // Initialize Linear Regression model and fit it to the country's data
    val lr = new LinearRegression()
      .setLabelCol("Total")
      .setFeaturesCol("features")
    val model = lr.fit(countryData)
    // Extract the slope (coefficient) representing the rate of increase for each country
    val slope = model.coefficients(0)
    (countryName, slope) // Return country name and its respective slope
  }
  // Filter top 10 countries with the highest positive slopes
  val top10Increases = countrySlopes
    .filter(_._2 > 0) // Consider only positive slopes
    .sortBy(-_._2)    // Sort by slope in descending order
    .take(10)         // Take the top 10 countrieS
  // Display the top 10 countries with the highest increase
  top10Increases.foreach { case (country, slope) =>
    println(s"Country: $country, Slope: $slope")
  }

  // ANALYSIS 5 (CORRELATION)

  // Summarize CO2 emissions and Renewable Energy by Country and Year
  val summed_co2 = co2_data.groupBy("Country", "Year").agg(round(functions.sum("Total"), 2).alias("TotalCO2Amount"))
  summed_co2.show()
  val summed_re = re_data.groupBy("Country", "Year").agg(round(functions.sum("Values"), 2).alias("TotalREAmount"))
  summed_re.show()
  // Register the DataFrames as temporary SQL tables to perform SQL operations
  summed_co2.createOrReplaceTempView("co2_emissions")
  summed_re.createOrReplaceTempView("renewable_energy")
  // Join the aggregated CO2 emissions and Renewable Energy datasets based on Country and Year
  val query = """
    SELECT ce.Country, ce.Year, ce.TotalCO2Amount AS co2_emissions, re.TotalREAmount AS renewable_energy
    FROM co2_emissions AS ce
    JOIN renewable_energy AS re ON ce.Country = re.Country AND ce.Year = re.Year
  """
  // Execute the SQL query to create a correlation table
  val correlationTable: DataFrame = spark.sql(query)
  // Display the correlation table
  correlationTable.show()
  // Define columns for correlation calculation
  val column1 = "co2_emissions"
  val column2 = "renewable_energy"
  // Calculate the correlation coefficient between CO2 emissions and Renewable Energy
  val correlationCoefficient = correlationTable.stat.corr(column1, column2)
  println(s"Correlation Coefficient between co2_emissions and renewable_energy: $correlationCoefficient")
}

// Create a SparkSession instance named "Data Cleaning" to initialize Spark
val spark = SparkSession.builder()
  .appName("Data Cleaning") // Assign a descriptive name to the Spark application
  .getOrCreate()

// Define the path to the CO2 emissions dataset stored in HDFS
val datasetPath = "hdfs://nyu-dataproc-m/user/ak8827_nyu_edu/project/fossil_co2.csv"

// Load the CO2 dataset using 'loadCO2Dataset' function and the SparkSession
val data = loadDataset(spark, datasetPath) // Load the CO2 dataset into a DataFrame

// Clean the loaded CO2 emissions data using 'cleanCO2Data' function
val clean_data = cleanCO2Dataset(data) // Apply data cleaning operations to the CO2 emissions DataFrame

// Load, profile, and clean the Renewable Energy generation dataset
val dataset2Path = "hdfs://nyu-dataproc-m/user/ak8827_nyu_edu/project/re_generation.csv"
val data2 = loadDataset(spark, dataset2Path) // Load the Renewable Energy dataset into a DataFrame
val clean_data2 = cleanREDataset(data2) // Apply data cleaning operations to the Renewable Energy DataFrame

// Perform analytics using the cleaned CO2 emissions and Renewable Energy datasets
performAnalytics(clean_data, clean_data2) // Perform various analyses using the cleaned data

// Stop the SparkSession to release resources after completing data processing
spark.stop() // Terminate the SparkSession
