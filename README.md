# Neo4j Connector for Apache Spark

This repository contains the Neo4j Connector for Apache Spark.

## License

This org.neo4j.neo4j-connector-apache-spark is Apache 2 Licensed

## Generating Documentation from Source

```
cd doc
# Install NodeJS dependencies
npm install
# Generate HTML/CSS from asciidoc
./node_modules/.bin/antora docs.yml
# Start local server to browse docs
npm run start
```

This will open http://localhost:8000/ which will serve development docs.

## Building

Build `target/org.neo4j.neo4j-connector-apache-spark_2.11-4.0.0.jar` for Scala 2.11

    mvn clean package

## Integration with Apache Spark Applications

**spark-shell, pyspark, or spark-submit**

`$SPARK_HOME/bin/spark-shell --jars org.neo4j.neo4j-connector-apache-spark_2.11-4.0.0.jar`

`$SPARK_HOME/bin/spark-shell --packages org.neo4j.neo4j-contrib:org.neo4j.neo4j-connector-apache-spark_2.11:4.0.0`

**sbt**

If you use the [sbt-spark-package plugin](https://github.com/databricks/sbt-spark-package), in your sbt build file, add:

```scala spDependencies += "org.neo4j.neo4j-contrib/org.neo4j.neo4j-connector-apache-spark_2.11:4.0.0"```

Otherwise,

```scala
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies += "org.neo4j.neo4j-contrib" % "org.neo4j.neo4j-connector-apache-spark_2.11" % "4.0.0"
```  

**maven**  
In your pom.xml, add:   

```xml
<dependencies>
  <!-- list of dependencies -->
  <dependency>
    <groupId>org.neo4j.neo4j-contrib</groupId>
    <artifactId>org.neo4j.neo4j-connector-apache-spark_2.11</artifactId>
    <version>4.0.0</version>
  </dependency>
</dependencies>
<repositories>
  <!-- list of other repositories -->
  <repository>
    <id>SparkPackagesRepo</id>
    <url>http://dl.bintray.com/spark-packages/maven</url>
  </repository>
</repositories>
```
