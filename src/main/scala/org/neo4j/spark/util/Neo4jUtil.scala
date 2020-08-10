package org.neo4j.spark.util

import java.time._
import java.time.format.DateTimeFormatter
import java.util.Properties

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.{JsonSerializer, ObjectMapper, SerializerProvider}
import com.fasterxml.jackson.databind.module.SimpleModule
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericRowWithSchema, UnsafeArrayData, UnsafeMapData, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, DateTimeUtils}
import org.apache.spark.sql.sources.{And, EqualNullSafe, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Not, Or, StringContains, StringEndsWith, StringStartsWith}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.neo4j.cypherdsl.core.{Condition, Cypher}
import org.neo4j.driver.internal.{InternalIsoDuration, InternalNode, InternalPoint2D, InternalPoint3D, InternalRelationship}
import org.neo4j.driver.types.{Entity, Path}
import org.neo4j.driver.{Session, Transaction, Values}
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.util.Neo4jImplicits.EntityImplicits

import scala.collection.JavaConverters._

object Neo4jUtil {

  val NODE_ALIAS = "n"
  val INTERNAL_ID_FIELD = "<id>"
  val INTERNAL_LABELS_FIELD = "<labels>"
  val INTERNAL_REL_ID_FIELD = "<rel.id>"
  val INTERNAL_REL_TYPE_FIELD = "<rel.type>"
  val RELATIONSHIP_SOURCE_ALIAS = "source"
  val RELATIONSHIP_TARGET_ALIAS = "target"
  val INTERNAL_REL_SOURCE_ID_FIELD = s"<${Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS}.id>"
  val INTERNAL_REL_TARGET_ID_FIELD = s"<${Neo4jUtil.RELATIONSHIP_TARGET_ALIAS}.id>"
  val RELATIONSHIP_ALIAS = "rel"

  private val properties = new Properties()
  properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("neo4j-spark-connector.properties"))

  val unsupportedTransientCodes = Set("Neo.TransientError.Transaction.Terminated",
    "Neo.TransientError.Transaction.LockClientStopped") // use the same strategy for TransientException as in the driver

  def closeSafety(autoCloseable: AutoCloseable): Unit = {
    if (autoCloseable == null) {
      return Unit
    }

    try {
      autoCloseable match {
        case s: Session => if (s.isOpen) s.close()
        case t: Transaction => if (t.isOpen) t.close()
        case _ => autoCloseable.close()
      }
    } catch {
      case _ => throw new Exception("This exception should be logged") // @todo Log
    }
  }

  val mapper = new ObjectMapper()
  private val module = new SimpleModule("Neo4jApocSerializer")
  module.addSerializer(classOf[Path], new JsonSerializer[Path]() {
    override def serialize(path: Path,
                           jsonGenerator: JsonGenerator,
                           serializerProvider: SerializerProvider): Unit = jsonGenerator.writeString(path.toString)
  })
  module.addSerializer(classOf[Entity], new JsonSerializer[Entity]() {
    override def serialize(entity: Entity,
                           jsonGenerator: JsonGenerator,
                           serializerProvider: SerializerProvider): Unit = jsonGenerator.writeObject(entity.toMap())
  })
  mapper.registerModule(module)

  def convertFromNeo4j(value: Any, schema: DataType = null): Any = {
    if (schema != null && schema == DataTypes.StringType && value != null && !value.isInstanceOf[String]) {
      convertFromNeo4j(mapper.writeValueAsString(value), schema)
    } else {
      value match {
        case node: InternalNode => {
          val map = node.asMap()
          val structType = extractStructType(schema)
          val fields = structType
            .filter(field => field.name != Neo4jUtil.INTERNAL_ID_FIELD && field.name != Neo4jUtil.INTERNAL_LABELS_FIELD)
            .map(field => convertFromNeo4j(map.get(field.name), field.dataType))
          InternalRow.fromSeq(Seq(convertFromNeo4j(node.id()), convertFromNeo4j(node.labels())) ++ fields)
        }
        case rel: InternalRelationship => {
          val map = rel.asMap()
          val structType = extractStructType(schema)
          val fields = structType
            .filter(field => field.name != Neo4jUtil.INTERNAL_REL_ID_FIELD
              && field.name != Neo4jUtil.INTERNAL_REL_TYPE_FIELD
              && field.name != INTERNAL_REL_SOURCE_ID_FIELD
              && field.name != INTERNAL_REL_TARGET_ID_FIELD)
            .map(field => convertFromNeo4j(map.get(field.name), field.dataType))
          InternalRow.fromSeq(Seq(convertFromNeo4j(rel.id()),
            convertFromNeo4j(rel.`type`()),
            convertFromNeo4j(rel.startNodeId()),
            convertFromNeo4j(rel.endNodeId())) ++ fields)
        }
        case d: InternalIsoDuration => {
          val months = d.months()
          val days = d.days()
          val nanoseconds: Integer = d.nanoseconds()
          val seconds = d.seconds()
          InternalRow.fromSeq(Seq(UTF8String.fromString(SchemaService.DURATION_TYPE), months, days, seconds, nanoseconds, UTF8String.fromString(d.toString)))
        }
        case zt: ZonedDateTime => DateTimeUtils.instantToMicros(zt.toInstant)
        case dt: LocalDateTime => DateTimeUtils.instantToMicros(dt.toInstant(ZoneOffset.UTC))
        case d: LocalDate => d.toEpochDay.toInt
        case lt: LocalTime => {
          InternalRow.fromSeq(Seq(
            UTF8String.fromString(SchemaService.TIME_TYPE_LOCAL),
            UTF8String.fromString(lt.format(DateTimeFormatter.ISO_TIME))
          ))
        }
        case t: OffsetTime => {
          InternalRow.fromSeq(Seq(
            UTF8String.fromString(SchemaService.TIME_TYPE_OFFSET),
            UTF8String.fromString(t.format(DateTimeFormatter.ISO_TIME))
          ))
        }
        case p: InternalPoint2D => {
          val srid: Integer = p.srid()
          InternalRow.fromSeq(Seq(UTF8String.fromString(SchemaService.POINT_TYPE_2D), srid, p.x(), p.y(), null))
        }
        case p: InternalPoint3D => {
          val srid: Integer = p.srid()
          InternalRow.fromSeq(Seq(UTF8String.fromString(SchemaService.POINT_TYPE_3D), srid, p.x(), p.y(), p.z()))
        }
        case l: java.util.List[_] => {
          val elementType = if (schema != null) schema.asInstanceOf[ArrayType].elementType else null
          ArrayData.toArrayData(l.asScala.map(e => convertFromNeo4j(e, elementType)).toArray)
        }
        case map: java.util.Map[_, _] => {
          if (schema != null) {
            val mapType = schema.asInstanceOf[MapType]
            ArrayBasedMapData(map.asScala.map(t => (convertFromNeo4j(t._1, mapType.keyType), convertFromNeo4j(t._2, mapType.valueType))))
          } else {
            ArrayBasedMapData(map.asScala.map(t => (convertFromNeo4j(t._1), convertFromNeo4j(t._2))))
          }
        }
        case s: String => UTF8String.fromString(s)
        case _ => value
      }
    }
  }

  private def extractStructType(dataType: DataType): StructType = dataType match {
    case structType: StructType => structType
    case mapType: MapType => extractStructType(mapType.valueType)
    case arrayType: ArrayType => extractStructType(arrayType.elementType)
    case _ => throw new UnsupportedOperationException(s"$dataType not supported")
  }

  def convertFromSpark(value: Any, schema: StructField = null): AnyRef = value match {
    case date: java.sql.Date => convertFromSpark(date.toLocalDate)
    case timestamp: java.sql.Timestamp => convertFromSpark(timestamp.toInstant.atZone(ZoneOffset.UTC))
    case unsafeRow: UnsafeRow => {
      val structType = extractStructType(schema.dataType)
      val row = new GenericRowWithSchema(unsafeRow.toSeq(structType).toArray, structType)
      convertFromSpark(row)
    }
    case struct: GenericRowWithSchema => {
      if (struct.fieldIndex("type") == -1) {
        Values.NULL
      }
      struct.getAs[UTF8String]("type").toString match {
        case SchemaService.POINT_TYPE_2D => Values.point(struct.getAs[Number]("srid").intValue(),
          struct.getAs[Number]("x").doubleValue(),
          struct.getAs[Number]("y").doubleValue())
        case SchemaService.POINT_TYPE_3D => Values.point(struct.getAs[Number]("srid").intValue(),
          struct.getAs[Number]("x").doubleValue(),
          struct.getAs[Number]("y").doubleValue(),
          struct.getAs[Number]("z").doubleValue())
        case SchemaService.DURATION_TYPE => Values.isoDuration(struct.getAs[Number]("months").longValue(),
          struct.getAs[Number]("days").longValue(),
          struct.getAs[Number]("seconds").longValue(),
          struct.getAs[Number]("nanoseconds").intValue())
        case SchemaService.TIME_TYPE_OFFSET => Values.value(OffsetTime.parse(struct.getAs[String]("value")))
        case SchemaService.TIME_TYPE_LOCAL => Values.value(LocalTime.parse(struct.getAs[String]("value")))
        case _ => Values.NULL // check if it's correct
      }
    }
    case unsafeArray: UnsafeArrayData => {
      val sparkType = schema.dataType match {
        case arrayType: ArrayType => arrayType.elementType
        case _ => schema.dataType
      }
      val javaList = unsafeArray.toSeq[AnyRef](sparkType)
        .map(elem => convertFromSpark(elem, schema))
        .asJava
      Values.value(javaList)
    }
    case unsafeMapData: UnsafeMapData => { // Neo4j only supports Map[String, AnyRef]
      val mapType = schema.dataType.asInstanceOf[MapType]
      Values.value((0 to unsafeMapData.numElements() - 1)
        .map(i => (unsafeMapData.keyArray().getUTF8String(i).toString, unsafeMapData.valueArray().get(i, mapType.valueType)))
        .toMap[String, AnyRef]
        .mapValues(value => convertFromNeo4j(value))
        .asJava)
    }
    case string: UTF8String => convertFromSpark(string.toString)
    case _ => Values.value(value)
  }

  def flattenMap(map: java.util.Map[String, AnyRef],
                 prefix: String = ""): java.util.Map[String, AnyRef] = map.asScala.flatMap(t => {
    val key = if (prefix != "") s"$prefix.${t._1}" else t._1
    t._2 match {
      case nestedMap: Map[String, AnyRef] => flattenMap(nestedMap.asJava, key).asScala.toSeq
      case _ => Seq((key, t._2))
    }
  })
    .toMap
    .asJava

  def connectorVersion: String = properties.getOrDefault("version", "UNKNOWN").toString

  def mapSparkFiltersToCypher(filter: Filter, container: org.neo4j.cypherdsl.core.PropertyContainer, attributeAlias: Option[String] = None): Condition = {
    filter match {
      case eqns: EqualNullSafe => container.property(attributeAlias.getOrElse(eqns.attribute)).isEqualTo(Cypher.literalOf(eqns.value))
      case eq: EqualTo => container.property(attributeAlias.getOrElse(eq.attribute)).isEqualTo(Cypher.literalOf(eq.value))
      case gt: GreaterThan => container.property(attributeAlias.getOrElse(gt.attribute)).gt(Cypher.literalOf(gt.value))
      case gte: GreaterThanOrEqual => container.property(attributeAlias.getOrElse(gte.attribute)).gte(Cypher.literalOf(gte.value))
      case lt: LessThan => container.property(attributeAlias.getOrElse(lt.attribute)).lt(Cypher.literalOf(lt.value))
      case lte: LessThanOrEqual => container.property(attributeAlias.getOrElse(lte.attribute)).lte(Cypher.literalOf(lte.value))
      case in: In => {
        val values = in.values.map(Cypher.literalOf)
        container.property(attributeAlias.getOrElse(in.attribute)).in(Cypher.literalOf(values.toIterable.asJava))
      }
      case notNull: IsNotNull => container.property(attributeAlias.getOrElse(notNull.attribute)).isNotNull
      case isNull: IsNull => container.property(attributeAlias.getOrElse(isNull.attribute)).isNull
      case startWith: StringStartsWith => container.property(attributeAlias.getOrElse(startWith.attribute)).startsWith(Cypher.literalOf(startWith.value))
      case endsWith: StringEndsWith => container.property(attributeAlias.getOrElse(endsWith.attribute)).endsWith(Cypher.literalOf(endsWith.value))
      case contains: StringContains => container.property(attributeAlias.getOrElse(contains.attribute)).contains(Cypher.literalOf(contains.value))
      case not: Not => mapSparkFiltersToCypher(not.child, container, attributeAlias).not()
      // case or: Or => mapSparkFiltersToCypher(or.left, containerLeft, attributeAlias).or(mapSparkFiltersToCypher(or.right, containerRight, attributeAlias))
      // case and: And => mapSparkFiltersToCypher(and.left, containerLeft, attributeAlias).and(mapSparkFiltersToCypher(and.right, containerRight, attributeAlias))
      case filter@(_: Filter) => throw new IllegalArgumentException(s"Filter of type `${filter}` is not supported.")
    }
  }
}
