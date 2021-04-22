package org.neo4j.spark

import org.apache.spark.sql.streaming.StreamingQueryProgress
import org.hamcrest.Matchers
import org.junit.Assert.assertEquals
import org.junit.Test
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.driver.{Transaction, TransactionWork}

import java.util.concurrent.TimeUnit

class DataSourceStreamingReaderTSE extends SparkConnectorScalaBaseTSE {

  @Test
  def testReadStream(): Unit = {
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run("CREATE (n:Movie {title: 'My movie 0'})").consume()
        })

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", "Movie")
      .load()

    val query = stream.writeStream.format("console").start()
    query.awaitTermination(1000)
    Thread.sleep(1000)

    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run("CREATE (n:Movie {title: 'My movie 1'})").consume()
        })

    var lastProgress: StreamingQueryProgress = null
    Assert.assertEventually(new Assert.ThrowingSupplier[Boolean, Exception] {
      override def get(): Boolean = {
        lastProgress = query.lastProgress
        lastProgress != null
      }
    }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)

    query.stop()

    assertEquals(1, lastProgress.numInputRows)
  }

  @Test
  def testReadStreamWithNoValues(): Unit = {
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run("MATCH (n) DETACH DELETE n").consume()
        })

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", "Movie")
      .load()

    val query = stream.writeStream.format("console").start()

    query.awaitTermination(1500)

    Thread.sleep(1000)

    query.stop()

    Assert.assertEventually(new Assert.ThrowingSupplier[Boolean, Exception] {
      override def get(): Boolean = {
        query.lastProgress != null && query.lastProgress.numInputRows == 0
      }
    }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
  }
}

