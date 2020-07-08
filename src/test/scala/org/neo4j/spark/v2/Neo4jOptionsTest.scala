package org.neo4j.spark.v2

import org.junit.Test
import org.junit.Assert._
import org.neo4j.driver.Config.TrustStrategy

class Neo4jOptionsTest {

  import org.junit.Rule
  import org.junit.rules.ExpectedException

  val _expectedException: ExpectedException = ExpectedException.none

  @Rule
  def exceptionRule: ExpectedException = _expectedException

  @Test
  def testUrlIsRequired(): Unit = {
    val options: java.util.Map[String, String] = java.util.Map.of(
      "node", "Person"
    )

    _expectedException.expect(classOf[IllegalArgumentException])
    _expectedException.expectMessage("Parameter 'url' is required")

    new Neo4jOptions(options)
  }

  @Test
  def testQueryAndNodeShouldThrowError(): Unit = {
    val options: java.util.Map[String, String] = java.util.Map.of(
      "url", "bolt://localhost",
      "query", "MATCH n RETURN n",
      "node", "Person"
    )

    _expectedException.expect(classOf[IllegalArgumentException])
    _expectedException.expectMessage("You need to specify just one of these options: 'query', 'node', 'relationship'")

    new Neo4jOptions(options)
  }

  @Test
  def testQueryAndRelationshipShouldThrowError(): Unit = {
    val options: java.util.Map[String, String] = java.util.Map.of(
      "url", "bolt://localhost",
      "query", "MATCH n RETURN n",
      "relationship", "Knows"
    )

    _expectedException.expect(classOf[IllegalArgumentException])
    _expectedException.expectMessage("You need to specify just one of these options: 'query', 'node', 'relationship'")

    new Neo4jOptions(options)
  }

  @Test
  def testNodeAndRelationshipShouldThrowError(): Unit = {
    val options: java.util.Map[String, String] = java.util.Map.of(
      "url", "bolt://localhost",
      "node", "PERSON",
      "relationship", "Knows"
    )

    _expectedException.expect(classOf[IllegalArgumentException])
    _expectedException.expectMessage("You need to specify just one of these options: 'query', 'node', 'relationship'")

    new Neo4jOptions(options)
  }

  @Test
  def testQueryShouldHaveQueryType(): Unit = {
    val query: String = "MATCH n RETURN n"
    val options: java.util.Map[String, String] = java.util.Map.of(
      "url", "bolt://localhost",
      "query", query
    )

    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    assertEquals(QueryType.TYPE_QUERY, neo4jOptions.query.queryType)
    assertEquals(query, neo4jOptions.query.value)
  }

  @Test
  def testNodeShouldHaveNodeType(): Unit = {
    val label: String = "Person"
    val options: java.util.Map[String, String] = java.util.Map.of(
      "url", "bolt://localhost",
      "node", label
    )

    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    assertEquals(QueryType.TYPE_NODE, neo4jOptions.query.queryType)
    assertEquals(label, neo4jOptions.query.value)
  }

  @Test
  def testRelationshipShouldHaveRelationshipType(): Unit = {
    val relationship: String = "KNOWS"
    val options: java.util.Map[String, String] = java.util.Map.of(
      "url", "bolt://localhost",
      "node", relationship
    )

    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    assertEquals(QueryType.TYPE_NODE, neo4jOptions.query.queryType)
    assertEquals(relationship, neo4jOptions.query.value)
  }

  @Test
  def testDrierDefaults(): Unit = {
    val options: java.util.Map[String, String] = java.util.Map.of(
      "url", "bolt://localhost",
      "query", "MATCH n RETURN n"
    )

    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    assertEquals("neo4j", neo4jOptions.connection.database)
    assertEquals("none", neo4jOptions.connection.auth)
    assertEquals("", neo4jOptions.connection.username)
    assertEquals("", neo4jOptions.connection.password)
    assertEquals(false, neo4jOptions.connection.encryption)
    assertEquals(TrustStrategy.Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES, neo4jOptions.connection.trustStrategy)
    assertEquals("", neo4jOptions.connection.certificatePath)
    assertEquals(1000, neo4jOptions.connection.lifetime)
    assertEquals(1000, neo4jOptions.connection.timeout)
  }
}
