package org.neo4j.spark

import org.junit.Assert.{assertEquals, fail}
import org.junit.Test
import org.neo4j.spark.util.Validations

class VersionValidationTSE extends SparkConnectorScalaBaseTSE {

  @Test
  def testThrowsExceptionSparkVersionIsNotSupported(): Unit = {
    try {
      Validations.version(Seq("2.4"))
    } catch {
      case e: IllegalArgumentException =>
        assertEquals(
          """Your currentSpark version 3.1.1 is not supported by the current connector.
            |Please visit https://neo4j.com/developer/spark/overview/#_spark_compatibility to know which connector version you need.
            |""".stripMargin, e.getMessage)
      case e: Throwable => fail(s"should be thrown a ${classOf[IllegalArgumentException].getName}, got ${e.getClass} instead")
    }
  }

}
