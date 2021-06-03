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
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = {
            tx.run(s"CREATE (n:Test1_Movie {title: 'My movie 0', timestamp: localdatetime()})").consume()
          }
        })

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", "Test1_Movie")
      .option("streaming.timestamp.property", "timestamp")
      .load()

    query = stream.writeStream
      .format("memory")
      .queryName("testReadStream")
      .start()

    val total = 60

    val expected = (1 to total).map(index => Map(
      "<labels>" -> mutable.WrappedArray.make(Array("Test1_Movie")),
      "title" -> s"My movie $index"
    ))

    Executors.newSingleThreadExecutor().submit(new Runnable {
      override def run(): Unit = {
        (1 to total).foreach(index => {
          Thread.sleep(200)
          SparkConnectorScalaSuiteIT.session()
            .writeTransaction(new TransactionWork[ResultSummary] {
              override def execute(tx: Transaction): ResultSummary = {
                tx.run(s"CREATE (n:Test1_Movie {title: 'My movie $index', timestamp: localdatetime()})")
                  .consume()
              }
            })
        })
      }
    })

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
        actual.toList == expected.toList
      }
    }, Matchers.equalTo(true), 40L, TimeUnit.SECONDS)
  }

  @Test
  def testReadStreamWithRelationship(): Unit = {
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(
            """
              |CREATE (person:Test2_Person {age: 0, timestamp: localdatetime()})
              |CREATE (post:Test2_Post {hash: "hash0"})
              |CREATE (person)-[:LIKES]->(post)
              |""".stripMargin).consume()
        })

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "LIKES")
      .option("streaming.timestamp.property", "source.timestamp")
      .option("relationship.source.labels", "Test2_Person")
      .option("relationship.target.labels", "Test2_Post")
      .load()

    query = stream.writeStream
      .format("memory")
      .queryName("testReadStream")
      .start()

    val total = 100
    Executors.newSingleThreadExecutor().submit(new Runnable {
      override def run(): Unit = {
        (1 to total).foreach(index => {
          Thread.sleep(200)
          SparkConnectorScalaSuiteIT.session()
            .writeTransaction(new TransactionWork[ResultSummary] {
              override def execute(tx: Transaction): ResultSummary = {
                tx.run(
                  s"""
                     |CREATE (person:Test2_Person {age: $index, timestamp: localdatetime()})
                     |CREATE (post:Test2_Post {hash: "hash$index"})
                     |CREATE (person)-[:LIKES]->(post)
                     |""".stripMargin)
                  .consume()
              }
            })
        })
      }
    })

    val expected = (1 to total).map(index => Map(
      "<rel.type>" -> "LIKES",
      "<source.labels>"-> mutable.WrappedArray.make(Array("Test2_Person")),
      "source.age" -> index,
      "<target.labels>"-> mutable.WrappedArray.make(Array("Test2_Post")),
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
    }, Matchers.equalTo(true), 40L, TimeUnit.SECONDS)
  }

  @Test
  def testReadStreamWithQuery(): Unit = {
    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("streaming.timestamp.property", "timestamp")
      .option("query",
        """
          |MATCH (p:Test3_Person)
          |WHERE p.timestamp > $event.fromTimestamp
          |RETURN p.age AS age, p.timestamp AS timestamp
          |""".stripMargin)
      .load()

    query = stream.writeStream
      .format("memory")
      .queryName("testReadStream")
      .start()

    val total = 60
    Executors.newSingleThreadExecutor().submit(new Runnable {
      override def run(): Unit = {
        (1 to total).foreach(index => {
          Thread.sleep(200)
          SparkConnectorScalaSuiteIT.session()
            .writeTransaction(new TransactionWork[ResultSummary] {
              override def execute(tx: Transaction): ResultSummary = {
                tx.run(s"CREATE (person:Test3_Person) SET person.age = $index, person.timestamp = localdatetime()")
                  .consume()
              }
            })
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
    }, Matchers.equalTo(true), 40L, TimeUnit.SECONDS)
  }

  @Test
  def testReadStreamWithLabelsGetAll(): Unit = {
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = {
            tx.run(s"CREATE (n:Test4_Movie {title: 'My movie 0', timestamp: localdatetime()})").consume()
          }
        })

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", "Test4_Movie")
      .option("streaming.timestamp.property", "timestamp")
      .option("streaming.get.all", "true")
      .load()

    query = stream.writeStream
      .format("memory")
      .queryName("testReadStream")
      .start()

    val total = 60
    Executors.newSingleThreadExecutor().submit(new Runnable {
      override def run(): Unit = {
        (1 to total).foreach(index => {
          Thread.sleep(200)
          SparkConnectorScalaSuiteIT.session()
            .writeTransaction(new TransactionWork[ResultSummary] {
              override def execute(tx: Transaction): ResultSummary = {
                tx.run(s"CREATE (n:Test4_Movie {title: 'My movie $index', timestamp: localdatetime()})")
                  .consume()
              }
            })
        })
      }
    })

    val expected = (0 to total).map(index => Map(
      "<labels>" -> mutable.WrappedArray.make(Array("Test4_Movie")),
      "title" -> s"My movie $index"
    ))

    Assert.assertEventually(new Assert.ThrowingSupplier[Boolean, Exception] {
      override def get(): Boolean = {
        val df = ss.sql("select * from testReadStream order by timestamp")
        val collect = df.collect()
        df.show(100, truncate = false)
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
    }, Matchers.equalTo(true), 40L, TimeUnit.SECONDS)
  }

  @Test
  def testReadStreamWithRelationshipGetAll(): Unit = {
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(
            """
              |CREATE (person:Test5_Person {age: 0, timestamp: localdatetime()})
              |CREATE (post:Test5_Post {hash: "hash0"})
              |CREATE (person)-[:LIKES]->(post)
              |""".stripMargin).consume()
        })

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "LIKES")
      .option("streaming.timestamp.property", "source.timestamp")
      .option("streaming.get.all", "true")
      .option("relationship.source.labels", "Test5_Person")
      .option("relationship.target.labels", "Test5_Post")
      .load()

    query = stream.writeStream
      .format("memory")
      .queryName("testReadStream")
      .start()

    val total = 60
    Executors.newSingleThreadExecutor().submit(new Runnable {
      override def run(): Unit = {
        (1 to total).foreach(index => {
          Thread.sleep(200)
          SparkConnectorScalaSuiteIT.session()
            .writeTransaction(new TransactionWork[ResultSummary] {
              override def execute(tx: Transaction): ResultSummary = {
                tx.run(
                  s"""
                     |CREATE (person:Test5_Person {age: $index, timestamp: localdatetime()})
                     |CREATE (post:Test5_Post {hash: "hash$index"})
                     |CREATE (person)-[:LIKES]->(post)
                     |""".stripMargin)
                  .consume()
              }
            })
        })
      }
    })

    val expected = (1 to total).map(index => Map(
      "<rel.type>" -> "LIKES",
      "<source.labels>"-> mutable.WrappedArray.make(Array("Test5_Person")),
      "source.age" -> index,
      "<target.labels>"-> mutable.WrappedArray.make(Array("Test5_Post")),
      "target.hash" -> s"hash$index"
    ))

    Assert.assertEventually(new Assert.ThrowingSupplier[Boolean, Exception] {
      override def get(): Boolean = {
        val df = ss.sql("select * from testReadStream order by `source.timestamp`")
        val collect = df.collect()
        df.show(61, truncate = false)
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
        actual.toList == expected.toList
      }
    }, Matchers.equalTo(true), 40L, TimeUnit.SECONDS)
  }

}
