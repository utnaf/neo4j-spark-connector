package org.neo4j.spark.util

import org.apache.spark.sql.sources.{EqualTo, Not}
import org.junit.Test
import org.junit.Assert._
import org.neo4j.spark.util.Neo4jImplicits._

class Neo4jImplicitsTest {

  @Test
  def `should quote the string` {
    // given
    val value = "Test with space"

    // when
    val actual = value.quote

    // then
    assertEquals(s"`$value`", actual)
  }

  @Test
  def `should not re-quote the string` {
    // given
    val value = "`Test with space`"

    // when
    val actual = value.quote

    // then
    assertEquals(value, actual)
  }

  @Test
  def `should not quote the string` {
    // given
    val value = "Test"

    // when
    val actual = value.quote

    // then
    assertEquals(value, actual)
  }

  @Test
  def `should return attribute if filter has it` {
    // given
    val filter = EqualTo("name", "John")

    // when
    val attribute = filter.getAttribute

    // then
    assertTrue(attribute.isDefined)
  }

  @Test
  def `should not return attribute if filter doesn't have it` {
    // given
    val filter = Not(EqualTo("name", "John"))

    // when
    val attribute = filter.getAttribute

    // then
    assertFalse(attribute.isDefined)
  }
}
