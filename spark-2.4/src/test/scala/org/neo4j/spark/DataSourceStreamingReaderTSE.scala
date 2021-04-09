package org.neo4j.spark

import org.junit.Assert.assertTrue
import org.junit.Test
import org.neo4j.driver.{Transaction, TransactionWork}
import org.neo4j.driver.summary.ResultSummary

class DataSourceStreamingReaderTSE extends SparkConnectorScalaBaseTSE {

  @Test
  def testReadStream(): Unit = {
    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", "Movie")
      .load()

    val query = stream.writeStream.format("console").start()
    query.awaitTermination(2000)

    val count = stream.count()

    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run("CREATE (n:Movie {title: 'My movie'})").consume()
        })

    query.stop()

    assertTrue(stream.count() == count + 1)
  }
}

