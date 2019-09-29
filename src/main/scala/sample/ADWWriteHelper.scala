package main.scala.sample

import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types.{DataType, StringType, TimestampType}

object ADWWriteHelper extends JdbcDialect {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .master("local[*]")
      .getOrCreate()

    JdbcDialects.registerDialect(ADWWriteHelper)

    // ADW Spark Connector

    val config = Config(Map(
      "url" -> "**.database.windows.net",
      "databaseName" -> "devdb",
      "dbTable" -> "dbo.sample",
      "user" -> "admin",
      "password" -> "***"
    ))

    import spark.implicits._

    val sampleDF = Seq(
      (8, "Tom"),
      (9, "Sam"),
      (10, "Adam")
    ).toDF("id", "name")

    import org.apache.spark.sql.SaveMode
    sampleDF
      .write
      .mode(SaveMode.Overwrite)
      .sqlDB(config)

    JdbcDialects.unregisterDialect(ADWWriteHelper)
  }

  override def canHandle(url: String): Boolean = url.contains("jdbc:sqlserver")

  // In order to overcome issues with TEXT datatype
  override def getJDBCType(dt: DataType): Option[JdbcType] = {
    val answer = dt match {
      case TimestampType => Some(JdbcType("DATETIME", java.sql.Types.TIMESTAMP))
      case StringType => Some(JdbcType("VARCHAR(250)", java.sql.Types.VARCHAR))
      case _ => None
    }
    answer
  }
}

