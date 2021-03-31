package org.neo4j.spark

import org.junit.Test

class DataSourceStreamingReaderTSE extends SparkConnectorScalaBaseTSE {

  @Test
  def testReadStream(): Unit = {
    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", "bolt://localhost:7687")
      .option("authentication.type", "basic")
      .option("authentication.basic.username", "neo4j")
      .option("authentication.basic.password", "password")
      .option("labels", "Movie")
      .load()

    val query = stream.writeStream.format("console").start()

    query.awaitTermination()
  }

  @Test
  def testWriteStream(): Unit = {
    val rateDf = ss.readStream.format("rate").load()

    val query = rateDf.writeStream.format(classOf[DataSource].getName)
      .option("url", "bolt://localhost:7687")
      .option("authentication.type", "basic")
      .option("authentication.basic.username", "neo4j")
      .option("authentication.basic.password", "password")
      .option("labels", "Timestampo")
      .option("checkpointLocation", "/tmp/checkpoint")
      .start()

    query.awaitTermination()
  }
}
