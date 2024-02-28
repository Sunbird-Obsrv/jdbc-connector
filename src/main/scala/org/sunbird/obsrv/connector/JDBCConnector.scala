package org.sunbird.obsrv.connector

import com.typesafe.config.Config
import org.apache.spark.sql.functions.{col, max}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.sunbird.obsrv.connector.model.Models.ConnectorContext
import org.sunbird.obsrv.connector.source.{ISourceConnector, SourceConnector}

object JDBCConnector {

  def main(args: Array[String]): Unit = {
    SourceConnector.process(args, new JDBCSourceConnector)
  }
}

case class JDBCConfig(driver: IJDBCDriver, url: String, userName: String, userPwd: String, table: String, timestampColumn: String, batchSize: Int, numBatches: Int)

class JDBCSourceConnector extends ISourceConnector {

  override def getSparkConf(config: Config): Map[String, String] = {
    Map[String, String]()
  }

  override def process(spark: SparkSession, ctx: ConnectorContext, config: Config, metricFn: (String, Long) => Unit): Dataset[Row] = {
    val jdbcConfig = getJDBCConfig(config)
    val recordsCount = countNewRecords(spark, ctx, jdbcConfig)
    if (recordsCount > 0) {
      val batches = Math.min((recordsCount.toDouble / jdbcConfig.batchSize).ceil, jdbcConfig.numBatches).toInt
      fetchRecords(spark, ctx, jdbcConfig, batches)
    } else {
      spark.emptyDataFrame
    }
  }

  private def getDriver(dbType: String): IJDBCDriver = {
    dbType match {
      case "postgresql" => new PostgresDriver
      case _ => throw new Exception("")
    }
  }

  private def fetchRecords(spark: SparkSession, ctx: ConnectorContext, jdbcConfig: JDBCConfig, batches: Int): DataFrame = {
    val dfList = for (idx <- 1 to batches) yield {
      fetchBatch(spark, ctx, jdbcConfig, (idx - 1) * jdbcConfig.batchSize)
    }
    val df = dfList.reduce((a, b) => a.join(b))
    val lastTimestamp: Any = df.agg(max(col(jdbcConfig.timestampColumn))).head().get(0)
    val lastTimestampDF = getAllTimestampRecords(spark, ctx, jdbcConfig, lastTimestamp)
    jdbcConfig.driver.updateLastTimestamp(ctx, lastTimestamp)
    df.join(lastTimestampDF).distinct()
  }

  private def getJDBCConfig(config: Config): JDBCConfig = {
    val jdbcUrl = s"jdbc:${config.getString("source.database.type")}://${config.getString("source.database.host")}:${config.getString("source.database.port")}/${config.getString("source.database.name")}"
    JDBCConfig(
      driver = getDriver(config.getString("source.database.type")), url = jdbcUrl,
      userName = config.getString("source.database.username"), userPwd = config.getString("source.database.pwd"),
      table = config.getString("source.table"), timestampColumn = config.getString("source.timestamp-column"),
      batchSize = config.getInt("source.batch-size"), numBatches = config.getInt("source.max-batches")
    )
  }

  private def readData(spark: SparkSession, jdbcConfig: JDBCConfig, query: String): DataFrame = {
    spark.read.format("jdbc")
      .option("driver", jdbcConfig.driver.getName())
      .option("url", jdbcConfig.url)
      .option("user", jdbcConfig.userName)
      .option("password", jdbcConfig.userPwd)
      .option("query", query)
      .load()
  }

  private def countNewRecords(spark: SparkSession, ctx: ConnectorContext, jdbcConfig: JDBCConfig): Long = {
    val countQuery = jdbcConfig.driver.countQuery(jdbcConfig.table, jdbcConfig.timestampColumn, ctx.state.getState[AnyRef]("lastFetchTimestamp"))
    val df = readData(spark, jdbcConfig, countQuery)
    df.head().getAs[Long]("count")
  }

  private def fetchBatch(spark: SparkSession, ctx: ConnectorContext, jdbcConfig: JDBCConfig, offset: Int): DataFrame = {
    val selectQuery = jdbcConfig.driver.batchQuery(jdbcConfig.table, jdbcConfig.timestampColumn, offset, jdbcConfig.batchSize, ctx.state.getState[AnyRef]("lastFetchTimestamp"))
    readData(spark, jdbcConfig, selectQuery)
  }

  private def getAllTimestampRecords(spark: SparkSession, ctx: ConnectorContext, jdbcConfig: JDBCConfig, lastTimestamp: Any): DataFrame = {
    val selectQuery = jdbcConfig.driver.timeStampQuery(jdbcConfig.table, jdbcConfig.timestampColumn, lastTimestamp)
    readData(spark, jdbcConfig, selectQuery)
  }
}