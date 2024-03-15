# Databricks notebook source
# DBTITLE 1,Install Shell if Needed
# MAGIC %pip install sh

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
# MAGIC ##### Notice company name of customer 1, which we will show being updated in our Post-Update table later

# COMMAND ----------

# DBTITLE 1,Pre-Update Table 
display(remote_table)

# COMMAND ----------

# DBTITLE 1,Define Connection Properties
jdbcUrl = "jdbc:sqlserver://westontest.database.windows.net:1433;databaseName=test"
connectionProperties = {
  "user" : "username",
  "password" : "examplePassword",
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# DBTITLE 1,Executing SQL Update Command via JDBC Connection
from py4j.java_gateway import java_import

# Import the JDBC package
java_import(sc._gateway.jvm,"")

# Get the JDBC connection
connection = sc._gateway.jvm.java.sql.DriverManager.getConnection(jdbcUrl, connectionProperties["user"], connectionProperties["password"])

# Create a statement object
statement = connection.createStatement()

# Define your SQL Update command
sqlUpdate = """
UPDATE SalesLT.Customer
SET CompanyName = 'New Company Name'
WHERE CustomerID = 1
"""

# Execute the update command
statement.executeUpdate(sqlUpdate)

# Close the connection
statement.close()
connection.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Post-Update Table
# MAGIC ##### Notice company name of customer 1, which is shown updated compared to Pre-Update table

# COMMAND ----------

# DBTITLE 1,Post-Update Table
# CustomerID 1 Changed CompanyName to "New Company Name"
display(remote_table)
