package org.sunbird.obsrv.connector

import org.sunbird.obsrv.connector.model.Models.ConnectorContext

trait IJDBCDriver {

  def getName(): String

  def countQuery(table: String, timestampColumn: String, timestampOpt: Option[AnyRef]): String

  def batchQuery(table: String, timestampColumn: String, offset: Int, batchSize: Int, timestampOpt: Option[AnyRef]): String

  def timeStampQuery(table: String, timestampColumn: String, timestamp: Any): String

  def updateLastTimestamp(ctx: ConnectorContext, lastTimestamp: Any): Unit

}