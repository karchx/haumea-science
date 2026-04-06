package com.haumea.core

import org.apache.spark.sql.SparkSession
import java.sql.{DriverManager, Connection, ResultSet}

trait Pipeline {
  def run(spark: SparkSession, connection: Connection): Unit

  def getConnectionPg(): Connection = {
    val jdbcUrl = "jdbc:postgresql://postgres-postgresql.platform:5432/platform"
    val username = sys.env.getOrElse("USERNAME", "platform")
    val password = sys.env.getOrElse("PASSWORD", "changeme123")
    DriverManager.getConnection(jdbcUrl, username, password)
  }

  def getMetadata(connection: Connection, layer: String, jobName: String): Map[String, Any] = {
    val query = s"SELECT * FROM spark_apps_metadata WHERE layer = '$layer' AND pipeline = '$jobName'"
    
    var metadata: Map[String, Any] = Map()

    try {
      val statement = connection.createStatement()
      val resultSet: ResultSet = statement.executeQuery(query)

      if (resultSet.next()) {
        val metaDataDB = resultSet.getMetaData
        val columnCount = metaDataDB.getColumnCount

        metadata = (1 to columnCount).map { i =>
          val columnName = metaDataDB.getColumnName(i)
          val columnValue = resultSet.getObject(i)

          columnName -> columnValue
        }.toMap
      }
    } catch {
      case e: Exception =>
        println(s"Err postgres: ${e.getMessage}")
        Map.empty[String, String]
    }

    metadata
  }

  def updateMetadata(connection: Connection, id: String, current: java.sql.Date): Unit = {
    val query = s"UPDATE spark_apps_metadata SET current = '$current' WHERE id = '$id'"
    println(s"Ejecutando query de actualización: $query")
   
    try {
      val statement = connection.createStatement()
      statement.executeUpdate(query)
    } catch {
      case e: Exception =>
        println(s"Err update Postgres: ${e.getMessage}")
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()

    val connection = getConnectionPg()
    try {
      run(spark, connection)
      
    } finally {
      spark.stop()
      if (connection != null && !connection.isClosed) {
        connection.close()
      }
    }
  }
}
