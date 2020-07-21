import wvlet.log.LogSupport
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.regexp_replace

object IbmExercise extends LogSupport {

	/*
		credentials for both IBM COS and DB2 have to be passed via terminal using the below command
		sbt "run arguments"
		E.g: sbt "run access_key secret_key endpoint url username password"

		accessing the arguments in main function
		credentials of COS
		args(0) -------> access_key
		args(1) -------> secret_key
		args(2) -------> endpoint

		credentials of DB2
		args(3) -------> url
		args(4) -------> username
		args(5) -------> password
	*/

	def main(args: Array[String]) {
		
		var spark = SparkSession.builder.master("local").appName("IBM Exercise").getOrCreate()
		// makes a connection to IBM COS via Stocator
		connectionToCos(spark, args(0), args(1), args(2))
		// reads data from Cos and stores it in dataframe
		var dataframe = readDataFromCos(spark)
		// writes the dataframe into DB2 using JBLC driver
		val written_df : DataFrame = DB2_Operations("write",spark, dataframe, "jdbc", "FFL38638.EMPLOYEE_DATA", args(3), args(4), args(5))
		// reads the data from DB2 and loads it into dataframe
		var read_dataframe = DB2_Operations("read",spark, null, "jdbc", "FFL38638.EMPLOYEE_DATA", args(3), args(4), args(5))
		// performs the computation operation and stores the result into COS as parquet
		calculateResult(spark, read_dataframe)
		// reads the stored parquet files
		spark.read.parquet("cos://candidate-exercise.myCos/emp-data.csv/result0.parquet").show()
		info("Main : " + "result0.parquet read from COS")
		spark.read.parquet("cos://candidate-exercise.myCos/emp-data.csv/result1.parquet").show()
		info("Main : " + "result1.parquet read from COS")
		spark.stop()
	}

	// Function to read the data from COS
	def readDataFromCos(spark: SparkSession): DataFrame = {
	
		// Reading the emp-data.csv into the dataframe
		val csvPath : String  = "cos://candidate-exercise.myCos/emp-data.csv"
		try {
			val df : DataFrame = spark.read.format("csv")
				.option("header", "true")
				.option("inferSchema", "true")
				.load(csvPath)
			info("readDataFromCos : " + "Read file data " + csvPath + " from COS")
			return df
		} catch {
			case e : Throwable => error("readDataFromCos : " + "Error While reading file path " + csvPath, e)
			return spark.emptyDataFrame
		}
		
	}

	// Function to write data from COS
	def writeDataToCos(dataframe: DataFrame, filePath: String) {

		// Storing the data into IBM COS
		try {
			dataframe.write
				.mode("overwrite")
				.parquet(filePath)
			info("writeDataToCos : " + "Written Data to " + filePath)
		} catch {
			case e : Throwable => error("writeDataToCos : " + "Error While writing to file path " + filePath + " from COS", e)
		}
	}

	// Function makes connection to IBM cos
	def connectionToCos(spark: SparkSession, access_key: String, secret_key: String, endpoint: String) {
	
		val prefix : String = "fs.cos.myCos"
		// configuring the stocator using HMAC method
		try {
			spark.sparkContext.hadoopConfiguration.set("fs.cos.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem")
			spark.sparkContext.hadoopConfiguration.set("fs.stocator.scheme.list", "cos")
			spark.sparkContext.hadoopConfiguration.set("fs.stocator.cos.impl", "com.ibm.stocator.fs.cos.COSAPIClient")
			spark.sparkContext.hadoopConfiguration.set("fs.stocator.cos.scheme", "cos")
			spark.sparkContext.hadoopConfiguration.set(prefix + ".access.key",access_key)
			spark.sparkContext.hadoopConfiguration.set(prefix + ".secret.key",secret_key)
			spark.sparkContext.hadoopConfiguration.set(prefix + ".endpoint",endpoint)
			spark.sparkContext.hadoopConfiguration.set(prefix + ".v2.signer.type", "false")
			info("connectionToCos : " + "Connection to COS established!")
		} catch {
			case e  : Throwable => error("connectionToCos : " + "Error while establishing connection to COS", e)
		}
	}

	// Function to calculate query results
	def calculateResult(spark: SparkSession, dataframe: DataFrame) {

		// creating a temporary table for computation
		dataframe.registerTempTable("employees")
		// query to calcualte the gender ratio (Male/Female) of each department
		val query_1 : String = """Select Department, 
									sum(case when gender='Male' then 1 else 0 end)/
									sum(case when gender='Female' then 1 else 0 end) as GenderRatio 
									from employees group by Department"""
		try {
			val genderRatio : DataFrame = spark.sql(query_1)
			info("calcualteResult : " + "Query 1 Processed")
			val filePath1 : String = "cos://candidate-exercise.myCos/emp-data.csv/result0.parquet"
			// writing the result into IBM COS as parquet file
			writeDataToCos(genderRatio, filePath1)
		} catch {
			case e  : Throwable => error("calcualteResult : " + "Error while processing Query 1", e)
		}
		// creating a temporary table with salary converted into float for performing math operations
		var dataframe_cleaned = spark.sql("Select Department, Gender, substring(Salary, 2) as Salary from employees")
		dataframe_cleaned = dataframe_cleaned.withColumn("Salary", regexp_replace(dataframe_cleaned("Salary"), "\\,", "").cast("float"))
		dataframe_cleaned.registerTempTable("employees2")
		// computes the average salary and salary gap(male - female) of each department
		val query_2 : String = """ Select Department, avg(Salary) as Salary_avg, 
									avg(case when Gender='Male' then Salary end) - avg(case when Gender='Female' 
									then Salary end) as Salary_gap from employees2 group by Department"""
		try {
			val averageSalary : DataFrame = spark.sql(query_2)
			info("calcualteResult : " + "Query 2 Processed")
			val filePath2 : String = "cos://candidate-exercise.myCos/emp-data.csv/result1.parquet"
			// writing the result into IBM COS as parquet file
			writeDataToCos(averageSalary, filePath2)
		} catch {
			case e  : Throwable => error("calcualteResult : " + "Error while processing Query 2", e)
		}
	}

	// Function to perform operation on DB2
	def DB2_Operations(operation: String, spark: SparkSession, jdbcDF: DataFrame, format: String, dbtable: String, 
	url: String, username: String, password: String) : DataFrame = {

		// match case to perform read or write operation
		operation.trim() match {
			case "read" => {
				try {
					val jdbcDF_read : DataFrame = spark.read
						.format(format)
						.option("url", url)
						.option("dbtable", dbtable)
						.option("user", username)
						.option("password", password)
						.load()
					info("DB2_Operations : " + "Read data from table " + dbtable + " from DB2")
					return jdbcDF_read

				} catch {
					case e  : Throwable => error("DB2_Operations : " + "Error while reading data from table " + dbtable + " from DB2", e)
					return spark.emptyDataFrame
				}
			}
			case "write" => {
				try {
					jdbcDF.write
						.format(format)
						.option("url", url)
						.option("dbtable", dbtable)
						.option("user", username)
						.option("password", password)
						.save()
					info("DB2_Operations : " + "Written data to table " + dbtable + " in DB2")
					return jdbcDF
				} catch {
					case e  : Throwable => error("DB2_Operations : " + "Error while writing data to table " + dbtable + " in DB2", e)
					return spark.emptyDataFrame
				}
			}
			case _ => {
				return jdbcDF
			}
		}

	}

}