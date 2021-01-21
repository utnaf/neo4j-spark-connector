package org.neo4j.util

import org.junit.Test

class Neo4jUtilTest {

  @Test
  def testSafetyCloseShouldNotFailWithNull(): Unit = {
    Neo4jUtil.closeSafety(null)
  }
}
