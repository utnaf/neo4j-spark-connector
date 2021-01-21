package org.neo4j

import java.time.Duration
import java.util.concurrent.{Callable, TimeUnit}

import org.neo4j.driver.{AuthToken, AuthTokens, GraphDatabase, SessionConfig}
import org.neo4j.TestUtil
import org.rnorth.ducttape.unreliables.Unreliables
import org.testcontainers.containers.Neo4jContainer
import org.testcontainers.containers.wait.strategy.{AbstractWaitStrategy, WaitAllStrategy}

import collection.JavaConverters._

class DatabasesWaitStrategy(private val auth: AuthToken) extends AbstractWaitStrategy {
  private var databases = Seq.empty[String]

  def forDatabases(dbs: Seq[String]): DatabasesWaitStrategy = {
    databases ++= dbs
    this
  }

  override def waitUntilReady() {
    val boltUrl = s"bolt://${waitStrategyTarget.getContainerIpAddress}:${waitStrategyTarget.getMappedPort(7687)}"
    val driver = GraphDatabase.driver(boltUrl, auth)
    val systemSession = driver.session(SessionConfig.forDatabase("system"))
    val tx = systemSession.beginTransaction()
    try {
      databases.foreach { db => tx.run(s"CREATE DATABASE $db IF NOT EXISTS") }
      tx.commit()
    } finally {
      tx.close()
    }

    Unreliables.retryUntilSuccess(startupTimeout.getSeconds.toInt, TimeUnit.SECONDS, new Callable[Boolean] {
      override def call(): Boolean = {
        getRateLimiter.doWhenReady(new Runnable {
          override def run(): Unit = {
            if (databases.nonEmpty) {
              val tx = systemSession.beginTransaction()
              val databasesStatus = try {
                tx.run("SHOW DATABASES").list().asScala.map(db => {
                  (db.get("name").asString(), db.get("currentStatus").asString())
                }).toMap
              } finally {
                tx.close()
              }

              val notOnline = databasesStatus.filter(it => {
                it._2 != "online"
              })

              if (databasesStatus.size < databases.size || notOnline.nonEmpty) {
                throw new RuntimeException(s"Cannot started because of the following databases: ${notOnline.keys}")
              }
            }
          }
        })
        true
      }
    })
    systemSession.close()
    driver.close()
  }
}

// docker pull org.neo4j.neo4j/org.neo4j.neo4j-experimental:4.0.0-rc01-enterprise
class Neo4jContainerExtension(imageName: String = s"neo4j${if (TestUtil.experimental()) "/neo4j-experimental" else ""}:${TestUtil.neo4jVersion()}-enterprise")
  extends Neo4jContainer[Neo4jContainerExtension](imageName) {
  private var databases = Seq.empty[String]

  def withDatabases(dbs: Seq[String]): Neo4jContainerExtension = {
    databases ++= dbs
    this
  }

  private def createAuth(): AuthToken = if (getAdminPassword.isEmpty) AuthTokens.basic("neo4j", getAdminPassword) else AuthTokens.none()

  override def start(): Unit = {
    if (databases.nonEmpty) {
      val waitAllStrategy = waitStrategy.asInstanceOf[WaitAllStrategy]
      waitAllStrategy.withStrategy(new DatabasesWaitStrategy(createAuth()).forDatabases(databases).withStartupTimeout(Duration.ofMinutes(2)))
    }
    addEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
    super.start()
  }
}
