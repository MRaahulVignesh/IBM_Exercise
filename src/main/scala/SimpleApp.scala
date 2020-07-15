package com.ibm.stocator;

import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object SimpleApp {
  def main(args: Array[String]) {
    
    var spark = SparkSession.builder.master("local").appName("Simple Application").getOrCreate()
    spark = connectionToCos(spark)

    calculateResult(spark)

    
    

    // Writing the Dataframe into DB2 
    //val written_df = DB2_Operations("write",spark, df, "jdbc", "FFL38638.EMPLOYEE_DATA", url, username, password)

    
    // dataframe.show()

    spark.stop()
  }

  def readDataFromCos(spark: SparkSession): DataFrame = {
    connectionToCos(spark)

    // Reading the emp-data.csv into the dataframe
    val csvPath = "cos://candidate-exercise.myCos/emp-data.csv"
    val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(csvPath)

    return df
  }

  // Function makes connection to IBM cos
  def connectionToCos(spark: SparkSession) : SparkSession = {
    val prefix = "fs.cos.myCos"

    // configuring the stocator using HMAC method
    spark.sparkContext.hadoopConfiguration.set("fs.cos.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.stocator.scheme.list", "cos")
    spark.sparkContext.hadoopConfiguration.set("fs.stocator.cos.impl", "com.ibm.stocator.fs.cos.COSAPIClient")
    spark.sparkContext.hadoopConfiguration.set("fs.stocator.cos.scheme", "cos")

    spark.sparkContext.hadoopConfiguration.set(prefix + ".access.key","0aba66146f3b450cacebaa908046d17e")
    spark.sparkContext.hadoopConfiguration.set(prefix + ".secret.key","27b804de3b329a680dbf148fd76da208f33e8a5aaaea4cbd")
    spark.sparkContext.hadoopConfiguration.set(prefix + ".endpoint", "s3.us.cloud-object-storage.appdomain.cloud")
    spark.sparkContext.hadoopConfiguration.set(prefix + ".v2.signer.type", "false")

    return spark

  }

  def calculateResult(spark: SparkSession) {

    val url = "jdbc:db2://dashdb-txn-sbox-yp-lon02-06.services.eu-gb.bluemix.net:50000/BLUDB"
    val username = "ffl38638"
    val password = "28cp+z39v9n8fm3r"

    var dataframe = DB2_Operations("read",spark, null, "jdbc", "FFL38638.EMPLOYEE_DATA", url, username, password)
    dataframe.registerTempTable("employees")

    // this query calcualtes the department specific gender ratio (Male/Female)
    // var genderRatio = spark.sql("Select Department, sum(case when gender='Male' then 1 else 0 end)/sum(case when gender='Female' then 1 else 0 end) as GenderRatio from employees group by Department")
    // genderRatio.show()

    var dataframe_cleaned = spark.sql("Select Department, Gender, substring(Salary, 2) as Salary from employees")
    dataframe_cleaned = dataframe_cleaned.withColumn("Salary", regexp_replace(dataframe_cleaned("Salary"), "\\,", "").cast("double"))
    dataframe_cleaned.registerTempTable("employees2")
    
    var averageSalary = spark.sql("Select Department, avg(Salary) as avg_Salary from employees2 group by Department")
    averageSalary.show()

    
  }


  def DB2_Operations(operation: String, spark: SparkSession, jdbcDF: DataFrame, format: String, dbtable: String, 
  url: String, username: String, password: String) : DataFrame = {
    
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