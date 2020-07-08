package org.neo4j.spark.v2

import org.neo4j.driver.Config.TrustStrategy

class Neo4jOptions(parameters: java.util.Map[String, String]) {

  import Neo4jOptions._
  import QueryType._

  private def getRequiredParameter(parameter: String): String = {
    if (!parameters.containsKey(parameter) || parameters.get((parameter)).isEmpty) {
      throw new IllegalArgumentException(s"Parameter '$parameter' is required")
    }

    parameters.get(parameter)
  }

  private def getParameter(parameter: String): Option[String] = {
    if (!parameters.containsKey(parameter) || parameters.get((parameter)).isEmpty) {
      return None
    }

    Some(parameters.get(parameter).trim())
  }

  val query: QueryOption = (getParameter(QUERY), getParameter(NODE), getParameter(RELATIONSHIP)) match {
    case (Some(query), None, None) => new QueryOption(TYPE_QUERY, query)
    case (None, Some(node), None) => new QueryOption(TYPE_NODE, node)
    case (None, None, Some(relationship)) => new QueryOption(TYPE_RELATIONSHIP, relationship)
    case _ => throw new IllegalArgumentException(s"You need to specify just one of these options: '$QUERY', '$NODE', '$RELATIONSHIP'")
  }

  val connection: Neo4jOptionsDriver = new Neo4jOptionsDriver(
    getRequiredParameter(URL),
    getParameter(DATABASE) getOrElse DEFAULT_DATABASE,
    getParameter(AUTH_TYPE) getOrElse DEFAULT_AUTH_TYPE,
    getParameter(AUTH_BASIC_USERNAME) getOrElse DEFAULT_AUTH_BASIC_USERNAME,
    getParameter(AUTH_BASIC_PASSWORD) getOrElse DEFAULT_AUTH_BASIC_PASSWORD,
    getParameter(ENCRYPTION_ENABLED) match {
      case Some(enabled) => if (enabled.equals("true")) true else false
      case _ => DEFAULT_ENCRYPTION_ENABLED
    },
    getParameter(ENCRYPTION_TRUST_STRATEGY) match {
      case Some(strategy) => TrustStrategy.Strategy.valueOf(strategy)
      case _ => DEFAULT_ENCRYPTION_TRUST_STRATEGY
    },
    getParameter(ENCRYPTION_CA_CERTIFICATE_PATH) getOrElse DEFAULT_ENCRYPTION_CA_CERTIFICATE_PATH,
    getParameter(MAX_CONNECTION_LIFETIME_MSECS) match {
      case Some(lifetime) => lifetime.toInt
      case _ => DEFAULT_MAX_CONNECTION_LIFETIME_MSECS
    },
    getParameter(MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS) match {
      case Some(timeout) => timeout.toInt
      case _ => DEFAULT_MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS
    }
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

  // query options
  val QUERY: String = "query"
  val NODE: String = "node"
  val RELATIONSHIP: String = "relationship"

  // defaults
  val DEFAULT_DATABASE = "neo4j"
  val DEFAULT_AUTH_TYPE = "none"
  val DEFAULT_AUTH_BASIC_USERNAME = ""
  val DEFAULT_AUTH_BASIC_PASSWORD = ""
  val DEFAULT_ENCRYPTION_ENABLED = false
  val DEFAULT_ENCRYPTION_TRUST_STRATEGY = TrustStrategy.Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES
  val DEFAULT_ENCRYPTION_CA_CERTIFICATE_PATH = ""
  val DEFAULT_MAX_CONNECTION_LIFETIME_MSECS = 1000
  val DEFAULT_MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS = 1000
}

object QueryType extends Enumeration {
  val TYPE_QUERY, TYPE_NODE, TYPE_RELATIONSHIP = Value
}