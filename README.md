# IBM_Exercise

## Question
Install and configure Eclipse using the other instructions provided, and then use Eclipse to develop a Scala program/project (using Scala and Spark) which implements the following functions/features:
1. Function that connects to IBM Cloud Object Store (COS) 
2. Function that reads a CSV file (emp-data.csv) from the COS bucket
3. Setup a DB2 database
   * Setup an account in IBM Public Cloud (cloud.ibm.com)
   * Create a simple DB2 database
   * HINT: you will need DB2 JDBC drivers for future steps(https://www.ibm.com/support/pages/db2-jdbc-driver-versions-and-downloads)
4. Write Scala code to:
   * Create a table based COS data schema read in Step 2 &
   * Write the contents from Step 2 to the table 
5. Write Scala code to read the same data from the database, calculate and display the following:
   * Gender ratio in each department
   * Average salary in each department
   * Male and female salary gap in each department
6. Scala code to write one the calculated data as a Parquet back to COS
7. (Optional) Build,compile and package this Scala job, then deploy to kubernates cluster with spark standalone cluster mode or spark on kubernates cluster mode.


## Avalible Scripts
sbt operations like sbt compile & sbt run
