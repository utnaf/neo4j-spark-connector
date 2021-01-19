package org.neo4j.spark.util


import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import javax.lang.model.SourceVersion
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.neo4j.driver.types.{Entity, Node, Relationship}
import org.neo4j.spark.service.SchemaService
import org.apache.spark.sql.sources.{EqualNullSafe, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Not, StringContains, StringEndsWith, StringStartsWith}
import scala.collection.JavaConverters._

object Neo4jImplicits {

  implicit class CypherImplicits(str: String) {
    private def isValidCypherIdentifier() = SourceVersion.isIdentifier(str) && !str.trim.startsWith("$")

    def quote(): String = if (!isValidCypherIdentifier() && !str.trim.startsWith("`") && !str.trim.endsWith("`")) s"`$str`" else str

    def unquote(): String = str.replaceAll("`", "");

    def removeAlias(): String = {
      val splatString = str.split('.')

      if (splatString.size > 1) {
        splatString.tail.mkString(".")
      }
      else {
        str
      }
    }
  }

  implicit class EntityImplicits(entity: Entity) {
    def toStruct(): StructType = {
      val fields = entity.asMap().asScala
        .groupBy(_._1)
        .map(t => {
          val value = t._2.head._2
          val cypherType = SchemaService.normalizedClassNameFromGraphEntity(value)
          StructField(t._1, SchemaService.cypherToSparkType(cypherType))
        })
      val entityFields = entity match {
        case node: Node => {
          Seq(StructField(Neo4jUtil.INTERNAL_ID_FIELD, DataTypes.LongType, nullable = false),
            StructField(Neo4jUtil.INTERNAL_LABELS_FIELD, DataTypes.createArrayType(DataTypes.StringType), nullable = true))
        }
        case relationship: Relationship => {
          Seq(StructField(Neo4jUtil.INTERNAL_REL_ID_FIELD, DataTypes.LongType, false),
            StructField(Neo4jUtil.INTERNAL_REL_TYPE_FIELD, DataTypes.StringType, false),
            StructField(Neo4jUtil.INTERNAL_REL_SOURCE_ID_FIELD, DataTypes.LongType, false),
            StructField(Neo4jUtil.INTERNAL_REL_TARGET_ID_FIELD, DataTypes.LongType, false))
        }
      }

      StructType(entityFields ++ fields)
    }

    def toMap(): java.util.Map[String, Any] = {
      val entityMap = entity.asMap().asScala
      val entityFields = entity match {
        case node: Node => {
          Map(Neo4jUtil.INTERNAL_ID_FIELD -> node.id(),
            Neo4jUtil.INTERNAL_LABELS_FIELD -> node.labels())
        }
        case relationship: Relationship => {
          Map(Neo4jUtil.INTERNAL_REL_ID_FIELD -> relationship.id(),
            Neo4jUtil.INTERNAL_REL_TYPE_FIELD -> relationship.`type`(),
            Neo4jUtil.INTERNAL_REL_SOURCE_ID_FIELD -> relationship.startNodeId(),
            Neo4jUtil.INTERNAL_REL_TARGET_ID_FIELD -> relationship.endNodeId())
        }
      }
      (entityFields ++ entityMap).asJava
    }
  }

  implicit class FilterImplicit(filter: Filter) {
    def getAttribute: Option[String] = Option(filter match {
      case eqns: EqualNullSafe => eqns.attribute
      case eq: EqualTo => eq.attribute
      case gt: GreaterThan => gt.attribute
      case gte: GreaterThanOrEqual => gte.attribute
      case lt: LessThan => lt.attribute
      case lte: LessThanOrEqual => lte.attribute
      case in: In => in.attribute
      case notNull: IsNotNull => notNull.attribute
      case isNull: IsNull => isNull.attribute
      case startWith: StringStartsWith => startWith.attribute
      case endsWith: StringEndsWith => endsWith.attribute
      case contains: StringContains => contains.attribute
      case not: Not => not.child.getAttribute.orNull
      case _ => null
    })

    def isAttribute(entityType: String): Boolean = {
      getAttribute.exists(_.contains(s"$entityType."))
    }

    def getAttributeWithoutEntityName: Option[String] = filter.getAttribute.map(_.unquote().split('.').tail.mkString("."))
  }

  implicit class StructTypeImplicit(structType: StructType) {
    def getFieldsName: Seq[String] = if (structType == null) {
      Seq.empty
    } else {
      structType.map(structField => structField.name)
    }

    def getMissingFields(fields: Set[String]): Set[String] = {
      val structFieldsNames = structType.getFieldsName
      fields.filterNot(structFieldsNames.contains(_))
    }
  }

}
