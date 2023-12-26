import org.apache.spark.sql.{SparkSession, DataFrame, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, OneHotEncoder}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator

def loadAndProfileDataset(spark: SparkSession, path: String): DataFrame = {
  val df = spark.read
    .option("header", "true")   // Use the first line of the CSV file as the header
    .csv(path)
    .withColumnRenamed("Avg. HH size", "AvgHHSize")

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



// Function to trim column names
def trimColumnNames(df: DataFrame): DataFrame = {
  df.columns.foldLeft(df)((currentDF, colName) => 
    currentDF.withColumnRenamed(colName, colName.trim)
  )
}


// Integrated cleaning function for the Renewable Energy generation data with unit standardization
def cleanRenewableDataset(df: DataFrame): DataFrame = {
  // Trim column names to remove any leading or trailing whitespace
  val trimmedDF = trimColumnNames(df)

  val filteredDF = trimmedDF.select("Years", "IRENA Menu Label", "Technology", "Type", "UN Region English", "Unit", "Values")  
    //Using universal naming of columns across datasets
    .withColumnRenamed("IRENA Menu Label", "Country")
    .withColumn("Country", lower(col("Country")))
    // Standardize units: Convert MW to GWh if necessary because we cannot take the "Values" column record without standardization
    .withColumn("Values", 
      when(col("Unit") === "GWh", col("Values"))
      // if the unit is MW it would be divided by 1000 to get GWh value
      .when(col("Unit") === "MW", col("Values") / lit(1000))
      .otherwise(col("Values")))
    .withColumn("Values", col("Values").cast("double"))
    .na.drop()
    .filter(col("Years").between(2006, 2017))

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


def cleanFinanceDataset(df: DataFrame): DataFrame = {
  // Remove rows with null values in any column
  val cleanedDF = df.na.drop().filter(col("Year") >= 2000 && col("Year") <= 2023)

  // Convert age column to integer and replace null values with a default value (e.g., 0)
  val outputDF = cleanedDF.withColumn("TotalFinanceFlow", col("Amount (2020 USD million)").cast("double"))

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

  // Update country names based on the mapping
  val mapCountryName = udf((country: String) => countryMapping.getOrElse(country.toLowerCase, country))
  val updatedData = outputDF.withColumn("Country", mapCountryName(lower(col("Country/Area"))))

  updatedData
}

def calculateCorrelation(df: DataFrame, col1: String, col2: String): Double = {
    // Use the corr method to calculate the correlation coefficient
    df.select(corr(col1, col2)).as[Double].first()
  }

// Create a Spark session
val spark = SparkSession.builder
  .appName("Big Data Project")
  .getOrCreate()

// Load, profile, and clean the first dataset
val dataset1Path = "project/renewable_data.csv" 
val df1 = loadAndProfileDataset(spark, dataset1Path)
val clean_df1 = cleanRenewableDataset(df1)

// Load, profile, and clean the second dataset
val dataset2Path = "project/investment_data.csv"
val df2 = loadAndProfileDataset(spark, dataset2Path)
val clean_df2 = cleanFinanceDataset(df2)


// Aggregate renewable_generated values by country and years
val aggregatedRenewableDF = clean_df1
  .groupBy("Country", "Years")
  .agg(functions.sum("Values").alias("TotalValues"))

val summedData = clean_df2.groupBy("Country", "Year")
  .agg(functions.sum("TotalFinanceFlow").alias("TotalAmount"))


aggregatedRenewableDF.show()

summedData.show()


//Trends in Renewable Energy Generation by UN Region over time
// Aggregate Renewable Energy Generation by UN Region
val aggregatedRenewableByRegion = clean_df1
  .groupBy("UN Region English", "Years")
  .agg(sum("Values").alias("TotalRenewableGenerated"))
  .orderBy("Years", "UN Region English")

// Show the aggregated renewable energy generation by UN Region
println(s"Analyze the distribution of renewable energy generation across different regions. ")
aggregatedRenewableByRegion.show()

val data_coalesced1 = aggregatedRenewableByRegion.coalesce(1)

// Write the results to a separate table
data_coalesced1.write.csv("project/aggregatedRenewableByRegion")


// Renewable Energy Generation by Type, On-grid or Off-grid.
// Aggregate Renewable Energy Generation by Type, On-grid or Off-grid
val aggregatedType = clean_df1
  .groupBy("Type")
  .agg(sum("Values").alias("TotalRenewableGenerated"))
  .orderBy("Type")

// Show the aggregated renewable energy generation by  by Type, On-grid or Off-grid
println(s"Analyze the distribution of renewable energy generation based on On-grid or Off-grid types. ")
aggregatedType.show()

val data_coalesced2 = aggregatedType.coalesce(1)

// Write the results to a separate table
data_coalesced2.write.csv("project/aggregatedType")

// Trends in renewable energy generation and investments across different countries
// Aggregate Renewable Energy Generation by Country
val aggregatedRenewableByCountry = clean_df1.groupBy("Country")
  .agg(sum("Values").alias("TotalRenewableGenerated"))

// Aggregate Investment by Country
val aggregatedInvestmentByCountry = clean_df2.groupBy("Country")
  .agg(sum("TotalFinanceFlow").alias("TotalInvestment"))

// Join the Aggregated Data
val combinedGeoData = aggregatedRenewableByCountry
  .join(aggregatedInvestmentByCountry, "Country")

// Show the combined data for analysis
println(s"Analyze the distribution of renewable energy generation and investments across different countries. ")
combinedGeoData.show()

val data_coalesced3 = combinedGeoData.coalesce(1)

// Write the results to a separate table
data_coalesced3.write.csv("project/combinedGeoData")


//SparkML
// Join the cleaned datasets to perform linear regression analytics model for Investment Impact Prediction using SparkML
val joinedDF = clean_df1.join(clean_df2, clean_df1("Years") === clean_df2("Year") && clean_df1("Country") === clean_df2("Country"))
  .select(
    clean_df2("TotalFinanceFlow"),
    clean_df1("Technology").alias("RenewableTechnology"),
    clean_df2("Region"),
    clean_df1("Values")
  )

// Feature Engineering
val featureCols = Array("TotalFinanceFlow", "RenewableTechnology", "Region")
val labelCol = "Values"

// StringIndexer and OneHotEncoder for categorical features
val technologyIndexer = new StringIndexer().setInputCol("RenewableTechnology").setOutputCol("TechnologyIndex")
val regionIndexer = new StringIndexer().setInputCol("Region").setOutputCol("RegionIndex")
val encoder = new OneHotEncoder().setInputCols(Array("TechnologyIndex", "RegionIndex")).setOutputCols(Array("TechnologyVec", "RegionVec"))

// VectorAssembler to create feature vector
val assembler = new VectorAssembler().setInputCols(Array("TotalFinanceFlow", "TechnologyVec", "RegionVec")).setOutputCol("features")

// Define the Linear Regression model
val lr = new LinearRegression().setLabelCol(labelCol).setFeaturesCol("features")

// Create a Pipeline
val pipeline = new Pipeline().setStages(Array(technologyIndexer, regionIndexer, encoder, assembler, lr))

// Split data into training and test sets
val Array(trainingData, testData) = joinedDF.randomSplit(Array(0.7, 0.3))

// Train the model
val model = pipeline.fit(trainingData)

// Make predictions
val predictions = model.transform(testData)

// Evaluate the model
val evaluator = new RegressionEvaluator().setLabelCol(labelCol).setPredictionCol("prediction").setMetricName("rmse")
val rmse = evaluator.evaluate(predictions)
println(s"Root Mean Squared Error (RMSE) on test data = $rmse")


// Finding top 10 countries by maximum RE generation
val topMaxREGeneration = clean_df1
  .groupBy("Country")
  .agg(max("Values").alias("MaxREGeneration"))
  .orderBy(desc("MaxREGeneration"))
  .limit(10)

println(s"Top 10 countries with highest RE generation")
topMaxREGeneration.show()

val data_coalesced4 = topMaxREGeneration.coalesce(1)

// Write the results to a separate table
data_coalesced4.write.csv("project/topMaxREGeneration")



// Register the DataFrames as a temporary SQL tables
aggregatedRenewableDF.createOrReplaceTempView("renewable")
summedData.createOrReplaceTempView("investments")

val query = """SELECT i.country as Country, i.year as Year, i.TotalAmount as investment_amount, r.TotalValues as renewable_generated
FROM investments as i
JOIN renewable as r ON i.country = r.country
"""

// How long does it take for an investment to take effect on renewable energy generation? We tried to test it for the future 5 years after the investment was made.
for (i <- 0 to 5) {
  val corrQuery = s"$query AND i.year = r.years - $i;"
  val correlationTable: DataFrame = spark.sql(corrQuery)

  // Show the results
  correlationTable.show()

  val column1 = "investment_amount"
  val column2 = "renewable_generated"

  // Calculate the correlation coefficient between the two columns
  val correlationCoefficient = calculateCorrelation(correlationTable, column1, column2)

  // Print the result
  println(s"Correlation Coefficient between $column1 and $column2: $correlationCoefficient")

  val data_coalesced5 = correlationTable.coalesce(1)

  // Write the results to a separate table
  data_coalesced5.write.csv(s"project/corr_renewbale_invest_$i")

}


// Analyzing Trends in Energy Generation by Technology over time
val technologyTrendsDF = clean_df1
  .groupBy("Technology", "Years")
  .agg(sum("Values").alias("TotalEnergyGenerated"))
  .orderBy("Years", "Technology")

// Show the technology trends
technologyTrendsDF.show()

println(s"Trends in energy generation by Technology categories over time")


val data_coalesced6 = technologyTrendsDF.coalesce(1)

// Write the results to a separate table
data_coalesced6.write.csv("project/technologyTrendsDF")


//val data_coalesced = correlationTable.coalesce(1)

// Write the results to a separate table
//data_coalesced.write.csv("project/corr_finance_co2")

// Stop the Spark session
spark.stop()
