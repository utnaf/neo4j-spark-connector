package org.neo4j.spark

import org.apache.spark.sql.streaming.StreamingQuery
import org.hamcrest.Matchers
import org.joda.time.LocalDateTime
import org.junit.{After, Test}
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.driver.{Transaction, TransactionWork}

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
  def testReadStreamWithLabels(): Unit = {
    Thread.sleep(1000)
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = {
            tx.run(s"CREATE (n:Movie {title: 'My movie 0', timestamp: datetime()})").consume()
          }
        })

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", "Movie")
      .option("streaming.timestamp.property", "timestamp")
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
                tx.run(s"CREATE (n:Movie {title: 'My movie $index', timestamp: localdatetime()})")
                  .consume()
              }
            })
          Thread.sleep(500)
        })
      }
    })

    val expected = (1 to total).map(index => Map(
      "<labels>" -> mutable.WrappedArray.make(Array("Movie")),
      "title" -> s"My movie $index"
    ))

    Assert.assertEventually(new Assert.ThrowingSupplier[Boolean, Exception] {
      override def get(): Boolean = {
        val df = ss.sql("select * from testReadStream order by timestamp")
        val collect = df.collect()
        val actual = if (!df.columns.contains("title")) {
          Array.empty
        } else {
          collect.map(row => Map(
            "<labels>" -> row.getAs[java.util.List[String]]("<labels>"),
            "title" -> row.getAs[String]("title")
          ))
        }
        actual.toList == expected.toList.slice(expected.size - actual.length, expected.size)
      }
    }, Matchers.equalTo(true), 30L, TimeUnit.SECONDS)
    Thread.sleep(1000)
  }

  @Test
  def testReadStreamWithRelationship(): Unit = {
    Thread.sleep(1000)
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(
            """
              |CREATE (person:Person {age: 0, timestamp: localdatetime()})
              |CREATE (post:Post {hash: "hash0"})
              |CREATE (person)-[:LIKES]->(post)
              |""".stripMargin).consume()
        })

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "LIKES")
      .option("streaming.timestamp.property", "source.timestamp")
      .option("relationship.source.labels", "Person")
      .option("relationship.target.labels", "Post")
      .load()

    query = stream.writeStream
      .format("memory")
      .queryName("testReadStream")
      .start()

    val total = 20
    Executors.newSingleThreadExecutor().submit(new Runnable {
      override def run(): Unit = {
        (1 to total).foreach(index => {
          SparkConnectorScalaSuiteIT.session()
            .writeTransaction(new TransactionWork[ResultSummary] {
              override def execute(tx: Transaction): ResultSummary = {
                tx.run(
                  s"""
                     |CREATE (person:Person {age: $index, timestamp: localdatetime()})
                     |CREATE (post:Post {hash: "hash$index"})
                     |CREATE (person)-[:LIKES]->(post)
                     |""".stripMargin)
                  .consume()
              }
            })
          Thread.sleep(100)
        })
      }
    })

    val expected = (1 to total).map(index => Map(
      "<rel.type>" -> "LIKES",
      "<source.labels>"-> mutable.WrappedArray.make(Array("Person")),
      "source.age" -> index,
      "<target.labels>"-> mutable.WrappedArray.make(Array("Post")),
      "target.hash" -> s"hash$index"
    ))

    Assert.assertEventually(new Assert.ThrowingSupplier[Boolean, Exception] {
      override def get(): Boolean = {
        val df = ss.sql("select * from testReadStream order by `source.timestamp`")
        val collect = df.collect()
        val actual: Array[Map[String, Any]] = if (!df.columns.contains("source.age") || !df.columns.contains("target.hash")) {
          Array.empty
        } else {
          collect.map(row => Map(
            "<rel.type>" -> row.getAs[java.util.List[String]]("<rel.type>"),
            "<source.labels>"-> row.getAs[java.util.List[String]]("<source.labels>"),
            "source.age" -> row.getAs[java.util.List[String]]("source.age"),
            "<target.labels>"-> row.getAs[java.util.List[String]]("<target.labels>"),
            "target.hash" -> row.getAs[java.util.List[String]]("target.hash")
          ))
        }
        actual.toList == expected.toList.slice(expected.size - actual.length, expected.size)
      }
    }, Matchers.equalTo(true), 30L, TimeUnit.SECONDS)
    Thread.sleep(1000)
  }

  @Test
  def testReadStreamWithQuery(): Unit = {
    Thread.sleep(1000)
    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("query",
        """
          |MATCH (p:Person)
          |WHERE p.timestamp >= $event.fromTimestamp AND p.timestamp <= $event.toTimestamp
          |RETURN p.age AS age
          |""".stripMargin)
      .load()

    query = stream.writeStream
      .format("memory")
      .queryName("testReadStream")
      .start()

    val total = 9
    Executors.newSingleThreadExecutor().submit(new Runnable {
      override def run(): Unit = {
        (1 to total).foreach(index => {
          SparkConnectorScalaSuiteIT.session()
            .writeTransaction(new TransactionWork[ResultSummary] {
              override def execute(tx: Transaction): ResultSummary = {
                tx.run(s"CREATE (person:Person) SET person.age = $index, person.timestamp = localdatetime()")
                  .consume()
              }
            })
          Thread.sleep(100)
        })
      }
    })

    val expected = (1 to total).map(index => Map("age" -> index.toString))

    Assert.assertEventually(new Assert.ThrowingSupplier[Boolean, Exception] {
      override def get(): Boolean = {
        val df = ss.sql("select * from testReadStream order by age")
        val collect = df.collect()
        val actual: Array[Map[String, Any]] = if (!df.columns.contains("age")) {
          Array.empty
        } else {
          collect.map(row => Map(
            "age" -> row.getAs[String]("age")
          ))
        }
        actual.toList == expected.toList.slice(expected.size - actual.length, expected.size)
      }
    }, Matchers.equalTo(true), 10L, TimeUnit.SECONDS)
  }
  Thread.sleep(1000)
}
