package test.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.Row

object RmSpike {

  val sparkSession = SparkSession.builder
    .master("local[*]").appName("test").getOrCreate

  val query = """
    
    
    """

  def main(args: Array[String]) {
    val df = fetch("prd-db-scan.acurian.com", "1521", "S_NUMTRA", "S_NUMTRA#2018", "oracle11g", "acuprd_app_numtra.acurian.com", query)
    
    df.printSchema
    
    var writer: DataFrameWriter[Row] = null
    writer = df.write.format("com.databricks.spark.csv")
    writer.option("header", "true")
      .option("delimiter", ",")
      .save("/spikeTest")
      
      
    val writtenData = sparkSession.sqlContext.read
        .format("com.databricks.spark.csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .option("delimiter", ",")
        .load("/spikeTest")
        
    writtenData.printSchema    
    
  }

  def fetch(host: String, port: String, username: String, password: String, dbType: String, databasename: String, query: String): DataFrame = {
    var prop = new java.util.Properties
    val url = getConnectionString(dbType, username, password, host, port, databasename)
    prop.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
    prop.setProperty("user", username)
    prop.setProperty("password", password)
    prop.setProperty("allowExisting", "true")
    val queryOrTable = s"(${query})"
    sparkSession.sqlContext.read.jdbc(url, queryOrTable, prop)
  }

  def getConnectionString(dbType: String, userName: String, password: String, host: String, port: String, dbName: String): String = {
    dbType.toLowerCase() match {
      case "mysql"     => s"jdbc:mysql://${host}:${port}/${dbName}?user=${userName}&password=${password}"
      case "postgres"  => s"jdbc:postgresql://${host}:${port}/?user=${userName}&password=${password}"
      case "oracle11g" => s"jdbc:oracle:thin:${userName}/${password}@${host}:${port}/${dbName}"
      case _           => ""
    }
  }

}