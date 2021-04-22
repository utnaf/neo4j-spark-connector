package org.neo4j.spark

import org.hamcrest.Matchers
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
    query.awaitTermination(2000)

    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run("CREATE (n:Movie {title: 'My movie 1'})").consume()
        })

    Assert.assertEventually(new Assert.ThrowingSupplier[Boolean, Exception] {
      override def get(): Boolean = {
        query.lastProgress != null && query.lastProgress.numInputRows == 1
      }
    }, Matchers.equalTo(true), 3, TimeUnit.SECONDS)

    query.stop()
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
    query.awaitTermination(2000)

    Assert.assertEventually(new Assert.ThrowingSupplier[Boolean, Exception] {
      override def get(): Boolean = {
        query.lastProgress != null && query.lastProgress.numInputRows == 0
      }
    }, Matchers.equalTo(true), 3, TimeUnit.SECONDS)

    query.stop()
  }
}

