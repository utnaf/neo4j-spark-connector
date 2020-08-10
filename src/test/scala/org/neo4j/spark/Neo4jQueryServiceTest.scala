package org.neo4j.spark

import org.apache.spark.sql.sources.{EqualTo, Filter, Not, Or}
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

  @Test
  def testRelationshipFilterEqualTo(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "false")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val filters: Array[Filter] = Array[Filter](
      EqualTo("source.name", "John Doe")
    )

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(filters)).createQuery()

    assertEquals("MATCH (source:`Person`) " +
      "MATCH (target:`Person`) " +
      "MATCH (source)-[rel:`KNOWS`]->(target) WHERE source.name = 'John Doe' RETURN source, rel, target", query)
  }

  @Test
  def testRelationshipFilterNotEqualTo(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "false")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val filters: Array[Filter] = Array[Filter](
      Or(EqualTo("source.name", "John Doe"), EqualTo("target.name", "John Doe"))
    )

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(filters)).createQuery()

    assertEquals("MATCH (source:`Person`) " +
      "MATCH (target:`Person`) " +
      "MATCH (source)-[rel:`KNOWS`]->(target) WHERE (source.name = 'John Doe' OR target.name = 'John Doe') RETURN source, rel, target", query)
  }
}
