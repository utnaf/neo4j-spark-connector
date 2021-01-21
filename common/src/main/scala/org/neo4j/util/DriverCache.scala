package org.neo4j.util

import org.neo4j.driver.{Driver, GraphDatabase}
import org.neo4j.util.DriverCache.{cache, jobIdCache}

import java.util.concurrent.ConcurrentHashMap
import java.util.{Collections, function}

class DriverCache(private val options: Neo4jDriverOptions, private val jobId: String) extends Serializable with AutoCloseable {
  def getOrCreate(): Driver = {
    this.synchronized {
      jobIdCache.add(jobId)
      cache.computeIfAbsent(options, new function.Function[Neo4jDriverOptions, Driver] {
        override def apply(t: Neo4jDriverOptions): Driver = GraphDatabase.driver(t.url, t.toNeo4jAuth, t.toDriverConfig)
      })
    }
  }

  def close(): Unit = {
    this.synchronized {
      jobIdCache.remove(jobId)
      if(jobIdCache.isEmpty) {
        val driver = cache.remove(options)
        if (driver != null) {
          Neo4jUtil.closeSafety(driver)
        }
      }
    }
  }
}

object DriverCache {
  private val cache: ConcurrentHashMap[Neo4jDriverOptions, Driver] = new ConcurrentHashMap[Neo4jDriverOptions, Driver]
  private val jobIdCache = Collections.newSetFromMap[String](new ConcurrentHashMap[String, java.lang.Boolean]())
}
