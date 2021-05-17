package org.neo4j.spark

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamingQuery
import org.hamcrest.Matchers
import org.junit.{After, Test}
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.driver.{SessionConfig, Transaction, TransactionWork}
import org.neo4j.spark.Assert.ThrowingSupplier

import java.util.UUID
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
            tx.run("CREATE (n:Movie {title: 'My movie 0'})").consume()
          }
        })

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", "Movie")
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
        val df = ss.sql("select * from testReadStream order by title")
        df.show()
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
    }, Matchers.equalTo(true), 30L, TimeUnit.SECONDS)
  }

  @Test
  def testReadStreamWithRelationship(): Unit = {
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(
            """
              |CREATE (person:Person {age: 0})
              |CREATE (post:Post {hash: "hash0"})
              |CREATE (person)-[:LIKES]->(post)
              |""".stripMargin).consume()
        })

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "LIKES")
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
                     |CREATE (person:Person {age: $index})
                     |CREATE (post:Post {hash: "hash$index"})
                     |CREATE (person)-[:LIKES]->(post)
                     |""".stripMargin)
                  .consume()
              }
            })
          Thread.sleep(200)
        })
      }
    })

    val expected = (0 to total).map(index => Map(
      "<rel.type>" -> "LIKES",
      "<source.labels>"-> mutable.WrappedArray.make(Array("Person")),
      "source.age" -> index,
      "<target.labels>"-> mutable.WrappedArray.make(Array("Post")),
      "target.hash" -> s"hash$index"
    ))

    Assert.assertEventually(new Assert.ThrowingSupplier[Boolean, Exception] {
      override def get(): Boolean = {
        val df = ss.sql("select * from testReadStream")
        val collect = df.collect()
        df.show()
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
    }, Matchers.equalTo(true), 30L, TimeUnit.SECONDS)
  }

  @Test
  def testReadStreamWithQuery(): Unit = {
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(new TransactionWork[ResultSummary] {
        override def execute(tx: Transaction): ResultSummary = {
          tx.run(s"CREATE (person:Person) SET person.age = 0")
            .consume()
        }
      })

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("query", "MATCH (p:Person) RETURN p.age AS age ORDER BY age")
      .option("query.count", "MATCH (p:Person) RETURN count(p) as count")
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
                tx.run(s"CREATE (person:Person) SET person.age = $index")
                  .consume()
              }
            })
          Thread.sleep(100)
        })
      }
    })

    val expected = (0 to total).map(index => Map("age" -> index))

    Assert.assertEventually(new Assert.ThrowingSupplier[Boolean, Exception] {
      override def get(): Boolean = {
        val df = ss.sql("select * from testReadStream order by age")
        val collect = df.collect()
        val actual: Array[Map[String, Any]] = if (!df.columns.contains("age")) {
          Array.empty
        } else {
          collect.map(row => Map(
            "age" -> row.getAs[java.util.List[String]]("age")
          ))
        }
        actual.toList == expected.toList
      }
    }, Matchers.equalTo(true), 30L, TimeUnit.SECONDS)
  }
}
