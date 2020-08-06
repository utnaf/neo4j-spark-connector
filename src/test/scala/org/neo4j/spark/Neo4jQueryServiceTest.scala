package org.neo4j.spark

import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.junit.Assert._
import org.junit.Test
import org.neo4j.spark.service.{Neo4jQueryReadStrategy, Neo4jQueryService}

class Neo4jQueryServiceTest {

  @Test
  def testNodeOneLabel(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(Array[Filter]())).createQuery()

    assertEquals("MATCH (n:`Person`) RETURN n", query)
  }

  @Test
  def testNodeMultipleLabels(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, ":Person:Player:Midfield")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(Array[Filter]())).createQuery()

    assertEquals("MATCH (n:`Person`:`Player`:`Midfield`) RETURN n", query)
  }

  @Test
  def testNodeFilterEqualTo(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val filters: Array[Filter] = Array[Filter](
      EqualTo("name", "John Doe")
    )

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(filters)).createQuery()

    assertEquals("MATCH (n:`Person`) WHERE n.name = 'John Doe' RETURN n", query)
  }
}
