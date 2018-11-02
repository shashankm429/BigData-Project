// Databricks notebook source
// Declare the values for your Azure SQL database
//To read data from Azure SQL Database
val jdbcUsername = "cloudsa"
val jdbcPassword = "Admin@123"
val jdbcHostname = "mysampleserveraw.database.windows.net" //typically, this is in the form or servername.database.windows.net
val jdbcPort = 1433
val jdbcDatabase ="mysampledatabase"

// COMMAND ----------

import java.util.Properties

val jdbc_url = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=60;"
val connectionProperties = new Properties()
connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")        

// COMMAND ----------

val sqlTableDF = spark.read.jdbc(jdbc_url, "SalesLT.Address", connectionProperties)

// COMMAND ----------

sqlTableDF.printSchema

// COMMAND ----------

sqlTableDF.show(10)

// COMMAND ----------

sqlTableDF.select("AddressLine1", "City").show(10)

// COMMAND ----------

import java.util.Properties

val jdbc_url = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=60;"
val connectionProperties = new Properties()
connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")

// COMMAND ----------

//Steps to read data from Azure Data Lake Storage
spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")


// COMMAND ----------

spark.conf.set("dfs.adls.oauth2.credential", "LMXNx/JCrMQYcNZ+rgqMzS0uSAyftm/uNwQt/DO28+o=") //application key


// COMMAND ----------

spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/e4d98dd2-9199-42e5-ba8b-da3e763ede2e/oauth2/token") //tenantid or directory id


// COMMAND ----------

val df = spark.read.json("adl://mystorefordatabricksdev.azuredatalakestore.net/small_radio_json.json") //data frame

// COMMAND ----------

df.show()

// COMMAND ----------

df.createOrReplaceTempView("temphvactable") //create temp table
spark.sql("create table hvactable_hive as select * from temphvactable")

// COMMAND ----------

//provide connection details to connect to AZURE SQL DB
val jdbcUsername = "cloudsa"
val jdbcPassword = "Admin@123"
val jdbcHostname = "mysampleserveraw.database.windows.net" //typically, this is in the form or servername.database.windows.net
val jdbcPort = 1433
val jdbcDatabase ="mysampledatabase"
//set the properties
import java.util.Properties

val jdbc_url = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=60;"
val connectionProperties = new Properties()
connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")
spark.table("hvactable_hive").write.jdbc(jdbc_url, "hvactable", connectionProperties)

// COMMAND ----------

