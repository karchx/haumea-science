package com.aries.core

import cats.effect.{IO, IOApp, Resource}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{functions => F}
import java.sql.{DriverManager, Connection, ResultSet}

trait Pipeline extends IOApp.Simple {
  def runPipeline(spark: SparkSession, connection: Connection): IO[Unit]

  private val connectionResource: Resource[IO, Connection] = {
    Resource.make {
      IO.blocking {
        val jdbcUrl = "jdbc:postgresql://postgres-postgresql.platform:5432/platform"
        val username = sys.env.getOrElse("USERNAME", "platform")
        val password = sys.env.getOrElse("PASSWORD", "changeme123")
        DriverManager.getConnection(jdbcUrl, username, password)
      }
    } { conn =>
      IO.blocking {
        if (conn != null && !conn.isClosed) conn.close()
      }
    }
  }

  private val sparkResource: Resource[IO, SparkSession] = {
    Resource.make {
      IO.delay {
        SparkSession.builder()
          .appName(this.getClass.getSimpleName.stripSuffix("$"))
          .getOrCreate()
      }
    } { spark =>
      IO.delay(spark.stop())
    }
  }

  def insertFactibleDate(df: DataFrame): DataFrame = {
    df.withColumn("fct_dt", F.make_date(F.col("year"), F.col("month"), F.col("day"))) 
  }

  def getMetadata(connection: Connection, layer: String, jobName: String): IO[Map[String, Any]] = IO.blocking {
    val query = s"SELECT * FROM spark_apps_metadata WHERE layer = '$layer' AND pipeline = '$jobName'"
    val statement = connection.createStatement()
    val resultSet: ResultSet = statement.executeQuery(query)

    if (resultSet.next()) {
      val metadatDB = resultSet.getMetaData
      val columnCount = metadatDB.getColumnCount
      (1 to columnCount).map { i =>
        metadatDB.getColumnName(i) -> resultSet.getObject(i)
      }.toMap
    } else {
      Map.empty[String, Any]
    }
  }.handleErrorWith { e =>
    IO.println(s"Error reader metadata in postgres ${e.getMessage}") *> IO.pure(Map.empty[String, Any])
  }

  def updateMetadata(connection: Connection, id: String, current: java.sql.Date): IO[Unit] = IO.blocking {
    val query = s"UPDATE spark_apps_metadata SET current = '$current' WHERE id = '$id'"
    val statement = connection.createStatement()
    statement.executeUpdate(query)
  }.handleErrorWith { e =>
    IO.println(s"Error update metadata in postgres ${e.getMessage}")
  }.void 

  override def run: IO[Unit] = {
    (for {
      spark <- sparkResource
      conn <- connectionResource
    } yield (spark, conn)).use { case (spark, conn)=>
      runPipeline(spark, conn)
    }
  }
}
