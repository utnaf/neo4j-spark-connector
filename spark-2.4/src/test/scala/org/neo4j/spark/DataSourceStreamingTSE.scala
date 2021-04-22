package org.neo4j.spark

import org.hamcrest.Matchers
import org.junit.Test
import org.neo4j.spark.Assert.ThrowingSupplier

import java.util.UUID
import java.util.concurrent.TimeUnit

class DataSourceStreamingTSE extends SparkConnectorScalaBaseTSE {

  @Test
  def testSinkStream(): Unit = {
    val rateDf = ss.readStream.format("rate")
      .option("rowsPerSecond", 1)
      .load()
    val query = rateDf.writeStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", "Timestamp")
      .option("checkpointLocation", "/tmp/checkpoint/" + UUID.randomUUID().toString)
      .start()

    Assert.assertEventually(new ThrowingSupplier[Boolean, Exception] {
      override def get(): Boolean = {
        val neo4jRateDf = ss.read.format(classOf[DataSource].getName)
          .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
          .option("labels", "Timestamp")
          .load()
        neo4jRateDf.collectAsList().size() > 0
      }
    }, Matchers.equalTo(true), 30L, TimeUnit.SECONDS)
    query.stop()
  }
}
