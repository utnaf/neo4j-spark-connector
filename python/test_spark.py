import pyspark
import unittest
from nose import with_setup
from testcontainers.neo4j import Neo4jContainer

class SparkTestCase(unittest.TestCase):

    spark_session = None
    neo4j_session = None
    neo4j_driver = None
    neo4j_container = None

    @classmethod
    def setUpClass(self):
        print("Setup...")
        with Neo4jContainer('neo4j:4.2') as neo4j_container:
            with neo4j_container.get_driver() as neo4j_driver:
                with neo4j_driver.session() as neo4j_session:
                    self.neo4j_session = neo4j_container
                    self.neo4j_driver = neo4j_driver
                    self.neo4j_session = neo4j_session

    @classmethod
    def tearDownClass(self):
        print("Tear down...")
        self.neo4j_session.close()
        self.neo4j_driver.close()
        self.neo4j_container.stop()

    def setUp(self):
        print("Erase DB")
        self.neo4j_session.run("MATCH (n) DETACH DELETE n")

    def testOne():
        assert True == True

    def testTwo():
        assert True == True