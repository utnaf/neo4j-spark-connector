package org.neo4j.spark.stream

import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.neo4j.spark.DataSource
import org.neo4j.spark.writer.Neo4jDataSourceWriter

import scala.collection.JavaConverters._

class Neo4jSink(private val options: Map[String, String], private val jobId: String, columns: Seq[String]) extends Sink {

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val dataSourceWriter = getDataSourceWriter(data.schema).createWriterFactory().createDataWriter(1, 1, batchId)

    data.write.format(classOf[DataSource].getName)
      .options(options)


    /*
    data.collect().map(row => row.getValuesMap(data.schema.fields.map(_.name)))
      .map(_.toSeq.map(_._1))
      .toSeq
      .foreach(row => dataSourceWriter.write(InternalRow.fromSeq(row)))*/
  }

  def getDataSourceWriter(schema: StructType) = new Neo4jDataSourceWriter(
    jobId,
    schema,
    SaveMode.Append,
    new DataSourceOptions(options.asJava)
  )
}
