package com.ibm.stocator;

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.regexp_replace

object SimpleApp {

	// Place your COS Credentials
	val access_key = ""
	val secret_key = ""
	val endpoint = ""

	// Place your DB2 Credentials
	val url = ""
	val username = ""
	val password = ""

	def main(args: Array[String]) {
		
		var spark = SparkSession.builder.master("local").appName("Simple Application").getOrCreate()
		// makes a connection to IBM COS via Stocator
		connectionToCos(spark)
		// reads data from Cos and stores it in dataframe
		var dataframe = readDataFromCos(spark)
		// writes the dataframe into DB2 using JBLC driver
		val written_df = DB2_Operations("write",spark, dataframe, "jdbc", "FFL38638.EMPLOYEE_DATA", url, username, password)
		// reads the data from DB2 and loads it into dataframe
		var read_dataframe = DB2_Operations("read",spark, null, "jdbc", "FFL38638.EMPLOYEE_DATA", url, username, password)
		// performs the computation operation and stores the result into COS as parquet
		calculateResult(spark, read_dataframe)
		// reads the stored parquet files
		spark.read.parquet("cos://candidate-exercise.myCos/emp-data.csv/result0.parquet").show()
		spark.read.parquet("cos://candidate-exercise.myCos/emp-data.csv/result1.parquet").show()
		spark.stop()
	}

	def readDataFromCos(spark: SparkSession): DataFrame = {
	
		// Reading the emp-data.csv into the dataframe
		val csvPath = "cos://candidate-exercise.myCos/emp-data.csv"
		val df = spark.read.format("csv")
		.option("header", "true")
		.option("inferSchema", "true")
		.load(csvPath)
		return df
	}

	def writeDataToCos(spark: SparkSession, dataframe: DataFrame, filePath: String): DataFrame = {

	    //connecting to IBM COS
	    var spark_temp = connectionToCos(spark)
	    // Storing the data into IBM COS
	    dataframe.write
			.mode("overwrite")
			.parquet(filePath)
		return dataframe
	}

	// Function makes connection to IBM cos
	def connectionToCos(spark: SparkSession) {
	
		val prefix = "fs.cos.myCos"
		// configuring the stocator using HMAC method
		spark.sparkContext.hadoopConfiguration.set("fs.cos.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem")
		spark.sparkContext.hadoopConfiguration.set("fs.stocator.scheme.list", "cos")
		spark.sparkContext.hadoopConfiguration.set("fs.stocator.cos.impl", "com.ibm.stocator.fs.cos.COSAPIClient")
		spark.sparkContext.hadoopConfiguration.set("fs.stocator.cos.scheme", "cos")
		spark.sparkContext.hadoopConfiguration.set(prefix + ".access.key",access_key)
		spark.sparkContext.hadoopConfiguration.set(prefix + ".secret.key",secret_key)
		spark.sparkContext.hadoopConfiguration.set(prefix + ".endpoint",endpoint)
		spark.sparkContext.hadoopConfiguration.set(prefix + ".v2.signer.type", "false")
	}

	// Function to calculate query results
	def calculateResult(spark: SparkSession, dataframe: DataFrame) {

		// creating a temporary table for computation
		dataframe.registerTempTable("employees")
		// query to calcualte the gender ratio (Male/Female) of each department
		val genderRatio = spark.sql("Select Department, sum(case when gender='Male' then 1 else 0 end)/sum(case when gender='Female' then 1 else 0 end) as GenderRatio from employees group by Department")
		// creating a temporary table with salary converted into float for performing math operations
		var dataframe_cleaned = spark.sql("Select Department, Gender, substring(Salary, 2) as Salary from employees")
		dataframe_cleaned = dataframe_cleaned.withColumn("Salary", regexp_replace(dataframe_cleaned("Salary"), "\\,", "").cast("float"))
		dataframe_cleaned.registerTempTable("employees2")
		// computes the average salary and salary gap(male - female) of each department
		val averageSalary = spark.sql("Select Department, avg(Salary) as Salary_avg, avg(case when Gender='Male' then Salary end) - avg(case when Gender='Female' then Salary end) as Salary_gap from employees2 group by Department")
		genderRatio.show()
		averageSalary.show()
		// writing the result into IBM COS as parquet file
		val filePath1 = "cos://candidate-exercise.myCos/emp-data.csv/result0.parquet"
		val filePath2 = "cos://candidate-exercise.myCos/emp-data.csv/result1.parquet"
		writeDataToCos(spark, genderRatio, filePath1)
		writeDataToCos(spark, averageSalary, filePath2)
	}

	// Function to perform operation on DB2
	def DB2_Operations(operation: String, spark: SparkSession, jdbcDF: DataFrame, format: String, dbtable: String, 
	url: String, username: String, password: String) : DataFrame = {

		// match case to perform read or write operation
		operation.trim() match {
			case "read" => {
				val jdbcDF = spark.read
					.format(format)
					.option("url", url)
					.option("dbtable", dbtable)
					.option("user", username)
					.option("password", password)
					.load()
				return jdbcDF
			}
			case "write" => {
				jdbcDF.write
					.format(format)
					.option("url", url)
					.option("dbtable", dbtable)
					.option("user", username)
					.option("password", password)
					.save()
				return jdbcDF
			}
			case _ => {
				return jdbcDF
			}
		}

	}

}