package org.neo4j.spark

import org.apache.spark.sql.streaming.StreamingQuery
import org.hamcrest.Matchers
import org.junit.{After, Test}
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.driver.{Transaction, TransactionWork, Values}

import java.util.Collections
import java.util.concurrent.{Executors, TimeUnit}
import scala.collection.mutable

class DataSourceStreamingReaderTSE extends SparkConnectorScalaBaseTSE {

  private var query: StreamingQuery = null

  @After
  def close(): Unit = {
    if (query != null) {
      query.stop()
    }
  }

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

    query = stream.writeStream
      .format("memory")
      .queryName("testReadStream")
      .start()

    val total = 10
    Executors.newSingleThreadExecutor().submit(new Runnable {
      override def run(): Unit = {
        (1 to total).foreach(index => {
          SparkConnectorScalaSuiteIT.session()
            .writeTransaction(new TransactionWork[ResultSummary] {
                override def execute(tx: Transaction): ResultSummary = {
                  tx.run(s"CREATE (n:Movie {title: 'My movie $index'})")
                    .consume()
                }
              })
          Thread.sleep(100)
        })
      }
    })

    val expected = (0 to total).map(index => Map(
      "<labels>" -> mutable.WrappedArray.make(Array("Movie")),
      "title" -> s"My movie $index"
    ))

    Assert.assertEventually(new Assert.ThrowingSupplier[Boolean, Exception] {
      override def get(): Boolean = {
        val df = ss.sql("select * from testReadStream")
        val collect = df.collect()
        val actual = if (!df.columns.contains("title")) {
          Array.empty
        } else {
          collect.map(row => Map(
            "<labels>" -> row.getAs[java.util.List[String]]("<labels>"),
            "title" -> row.getAs[String]("title")
          ))
        }
        actual.toList == expected.toList
      }
    }, Matchers.equalTo(true), 10, TimeUnit.SECONDS)
  }
}

