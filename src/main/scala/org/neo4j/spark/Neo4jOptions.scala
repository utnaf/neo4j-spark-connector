package org.neo4j.spark

import org.neo4j.driver.Config.TrustStrategy

class Neo4jOptions(private val parameters: java.util.Map[String, String]) {

  import Neo4jOptions._
  import QueryType._

  private def getRequiredParameter(parameter: String): String = {
    if (!parameters.containsKey(parameter) || parameters.get((parameter)).isEmpty) {
      throw new IllegalArgumentException(s"Parameter '$parameter' is required")
    }

    parameters.get(parameter)
  }

  private def getParameter(parameter: String, defaultValue: String = ""): String = {
    if (!parameters.containsKey(parameter) || parameters.get((parameter)).isEmpty) {
      return defaultValue
    }

    parameters.get(parameter).trim()
  }

  val query: QueryOption = (
    getParameter(QUERY.toString.toLowerCase),
    getParameter(NODE.toString.toLowerCase),
    getParameter(RELATIONSHIP.toString.toLowerCase())
  ) match {
    case (query, "", "") => QueryOption(QUERY, query)
    case ("", node, "") => QueryOption(NODE, node)
    case ("", "", relationship) => QueryOption(RELATIONSHIP, relationship)
    case _ => throw new IllegalArgumentException(
      s"You need to specify just one of these options: ${QueryType.values.toSeq.map( value => s"'${value.toString.toLowerCase()}'").sorted.mkString(", ")}"
    )
  }

  val connection: Neo4jOptionsDriver = Neo4jOptionsDriver(
    getRequiredParameter(URL),
    getParameter(DATABASE, DEFAULT_DATABASE),
    getParameter(AUTH_TYPE, DEFAULT_AUTH_TYPE),
    getParameter(AUTH_BASIC_USERNAME, DEFAULT_AUTH_BASIC_USERNAME),
    getParameter(AUTH_BASIC_PASSWORD, DEFAULT_AUTH_BASIC_PASSWORD),
    getParameter(ENCRYPTION_ENABLED, DEFAULT_ENCRYPTION_ENABLED.toString).toBoolean,
    TrustStrategy.Strategy.valueOf(getParameter(ENCRYPTION_TRUST_STRATEGY, DEFAULT_ENCRYPTION_TRUST_STRATEGY.name())),
    getParameter(ENCRYPTION_CA_CERTIFICATE_PATH, DEFAULT_ENCRYPTION_CA_CERTIFICATE_PATH),
    getParameter(MAX_CONNECTION_LIFETIME_MSECS, DEFAULT_MAX_CONNECTION_LIFETIME_MSECS.toString).toInt,
    getParameter(MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS, DEFAULT_MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS.toString).toInt
  )
}

case class QueryOption(queryType: QueryType.Value, value: String) extends Serializable

case class Neo4jOptionsDriver(
                               url: String,
                               database: String,
                               auth: String,
                               username: String,
                               password: String,
                               encryption: Boolean,
                               trustStrategy: TrustStrategy.Strategy,
                               certificatePath: String,
                               lifetime: Int,
                               timeout: Int
                             ) extends Serializable

object Neo4jOptions {

  // connection options
  val URL = "url"
  val DATABASE = "database"
  val AUTH_TYPE = "authentication.type"
  val AUTH_BASIC_USERNAME = "authentication.basic.username"
  val AUTH_BASIC_PASSWORD = "authentication.basic.password"
  val ENCRYPTION_ENABLED = "encryption.enabled"
  val ENCRYPTION_TRUST_STRATEGY = "encryption.trust.strategy"
  val ENCRYPTION_CA_CERTIFICATE_PATH = "encryption.ca.certificate.path"
  val MAX_CONNECTION_LIFETIME_MSECS = "connection.max.lifetime.msecs"
  val MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS = "connection.acquisition.timeout.msecs"

  // defaults
  val DEFAULT_DATABASE = "neo4j"
  val DEFAULT_AUTH_TYPE = "basic"
  val DEFAULT_AUTH_BASIC_USERNAME = ""
  val DEFAULT_AUTH_BASIC_PASSWORD = ""
  val DEFAULT_ENCRYPTION_ENABLED = false
  val DEFAULT_ENCRYPTION_TRUST_STRATEGY = TrustStrategy.Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES
  val DEFAULT_ENCRYPTION_CA_CERTIFICATE_PATH = ""
  val DEFAULT_MAX_CONNECTION_LIFETIME_MSECS = 1000
  val DEFAULT_MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS = 1000
}

object QueryType extends Enumeration {
  val QUERY, NODE, RELATIONSHIP = Value
}