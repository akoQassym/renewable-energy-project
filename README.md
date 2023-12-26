# The Effect of Renewable Energy Development on Carbon Emissions

## Author: Aknur Kassym

### Description
Welcome! This project aims to analyze global trends in renewable energy development and their impact on carbon emissions from 2000 to 2016. Leveraging advanced big data analytics tools like Hadoop and Spark, we delve into investment patterns, energy generation volumes, and global CO2 emissions to derive meaningful insights. Our analysis uncovers trends, patterns, and factors contributing to renewable energy development and fluctuations in carbon emissions.

### Folder Structure
The repository's structure includes various files for data ingestion, cleaning, ETL (Extract, Transform, Load), and profiling. All files are within the main repository, directly accessible without subfolders. The files are named: "ana_code_co2.scala", "data_ingest_co2.scala", "etl_code_co2.scala", and "profiling_code_co2.scala".

### Dataset Access
Access to datasets ("fossil_co2.csv", "re_generation.csv", "investment_data.csv") is provided for users: as17321_nyu_edu, lj2330_nyu_edu, and adm209_nyu_edu. These datasets reside in the user folder "project" under the ak8827 account.

### Running and Testing the Code
To engage with the analysis, ensure access to the datasets and then download the provided analysis codes available in the main repository. Please note that each team member worked on their section independently, resulting in different file paths to the datasets. Adjust these paths accordingly before executing the code.

### To Run the Code:
Use the following command after navigating to the directory where the code is stored:
spark-shell --deploy-mode client -i <name of the scala file>

Thank you for your interest and contributions!
