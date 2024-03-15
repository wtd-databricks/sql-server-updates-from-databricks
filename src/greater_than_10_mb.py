# Databricks notebook source
# DBTITLE 1,Install Libraries
# Install JDBC Libraries
%pip install pyodbc
%pip install jaydebeapi

# COMMAND ----------

# DBTITLE 1,Get Public Facing IP of Whatever Cluster You Are Running
# MAGIC %sh curl ifconfig.me
# MAGIC
# MAGIC # Gives public facing IP of whatever cluster you are running
# MAGIC # ifconfig.me returns the IP address you connected from
# MAGIC # Paste this into your list of allowed firewalls

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example of Whitelisting Cluster IP Address in Azure for SQL Server Firewall
# MAGIC #
# MAGIC ##### Notice above Cluster IP falls within range of firewall rule 'databricks-cluster'
# MAGIC #
# MAGIC <img src="https://i.imgur.com/MP9J47I.png" width="600" alt="Example of Whitelisting Cluster IP Address in Azure for SQL Server Firewall">

# COMMAND ----------

# DBTITLE 1,Read Remote SQL Server Table into DataFrame
# Read remote SQL Server using native spark connector
remote_table = (spark.read
  .format("sqlserver")
  .option("host", "westontest.database.windows.net")
  .option("port", "1433")
  .option("user", "username")
  .option("password", "examplePassword")
  .option("database", "test")
  .option("dbtable", "SalesLT.Customer")
  .load()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pre-Update Table
# MAGIC ##### Notice customers in row 1 & 3, which we will show being updated in our Post-Update table later

# COMMAND ----------

# DBTITLE 1,Pre-Update Table
display(remote_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's start by creating a DataFrame that represents new and updated customer data. 
# MAGIC ##### We then write those changes to the staging table SalesLT.Customer_Staging using JDBC
# MAGIC ##### In a real scenario, this could come from various sources like CSV files, external databases, or real-time data streams

# COMMAND ----------

# DBTITLE 1,Data Preparation and Identification of Changes with Spark
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Define the schema to match the SQL Server table
schema = StructType([
    StructField("CustomerID", IntegerType(), False),
    StructField("NameStyle", BooleanType(), True),
    StructField("Title", StringType(), True),
    StructField("FirstName", StringType(), True),
    StructField("MiddleName", StringType(), True),
    StructField("LastName", StringType(), True),
    StructField("Suffix", StringType(), True),
    StructField("CompanyName", StringType(), True),
    StructField("SalesPerson", StringType(), True),
    StructField("EmailAddress", StringType(), True),
    StructField("Phone", StringType(), True),
    StructField("PasswordHash", StringType(), True),
    StructField("PasswordSalt", StringType(), True),
    StructField("rowguid", StringType(), True),
    StructField("ModifiedDate", TimestampType(), True)
])

# Current timestamp in Python's datetime format
current_time = datetime.datetime.now()

# Sample data, where the last field is now a Python datetime, not a Column
data = [
    (1, False, "Mr.", "John", None, "Doe", None, "Acme Inc.", None, "john.doe@example.com", "555-0101", "newHash1", "newSalt1", str(uuid.uuid4()), current_time),
    (3, False, "Ms.", "Jane", "A.", "Smith", None, "Beta LLC", None, "jane.smith@example.com", "555-0102", "newHash2", "newSalt2", str(uuid.uuid4()), current_time),
    # Additional rows representing new or updated customers can be added here
]

# Create a DataFrame using the schema defined above
df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame to verify its contents
df.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Define Connection Properties and Write Processed Data to Staging Table
# Define JDBC Connection Properties
jdbcUrl = "jdbc:sqlserver://westontest.database.windows.net:1433;databaseName=test"
connectionProperties = {
  "user" : "username",
  "password" : "examplePassword",
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Writing DataFrame to a staging table
df.write.jdbc(url=jdbcUrl, table="SalesLT.Customer_Staging", mode="overwrite", properties=connectionProperties)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example of Stored Procedure in SQL Server Implementing Merge Logic
# MAGIC #
# MAGIC <img src="https://i.imgur.com/krCgLsy.png" width="600" alt="Example of Store Procedure in SQL Server">

# COMMAND ----------

# DBTITLE 1,Execute Stored Procedure (Merging Staging and Prod Tables)
import jaydebeapi

# Database connection properties
url = "jdbc:sqlserver://westontest.database.windows.net:1433;database=test"
username = "username"
password = "examplePassword"
driver = "/databricks/driver/sqljdbc42.jar"  # Path to the SQL Server JDBC driver jar file

# SQL command to execute the stored procedure
sql_command = "EXEC dbo.MergeCustomerData"  # Assuming 'dbo.MergeCustomerData' is your stored procedure

# Make the JDBC connection and execute the command
conn = jaydebeapi.connect("com.microsoft.sqlserver.jdbc.SQLServerDriver", url, [username, password], driver)
try:
    curs = conn.cursor()
    curs.execute(sql_command)
    curs.close()
finally:
    if conn:
        conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Post-Update Table
# MAGIC ##### Notice customers in row 1 & 3 have been updated with our Stored Procedure invoked Merge Operation of Staging & Main

# COMMAND ----------

# DBTITLE 1,Post - Update Table
remote_table = (spark.read
  .format("sqlserver")
  .option("host", "westontest.database.windows.net")
  .option("port", "1433") # optional, can use default port 1433 if omitted
  .option("user", "username")
  .option("password", "examplePassword")
  .option("database", "test")
  .option("dbtable", "SalesLT.Customer") # (if schemaName not provided, default to "dbo")
  .load()
)

display(remote_table)
