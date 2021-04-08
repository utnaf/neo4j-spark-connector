package org.neo4j.spark

import org.junit.Assert._
import org.junit.Test

import java.util.UUID

class DataSourceStreamingReaderTSE extends SparkConnectorScalaBaseTSE {

  @Test
  def testWriteStream(): Unit = {
    val rateDf = ss.readStream.format("rate")
      .option("rowsPerSecond", 1)
      .load()

    val query = rateDf.writeStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", "Timestamp")
      .option("checkpointLocation", "/tmp/checkpoint/" + UUID.randomUUID().toString)
      .start()

    query.awaitTermination(1000)
    query.processAllAvailable()
    query.stop()

    val neo4jRateDf = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", "Timestamp")
      .load()

    assertTrue(neo4jRateDf.collectAsList().size() > 0)
  }
}
