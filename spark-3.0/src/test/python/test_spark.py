import pyspark
from pyspark.sql import SparkSession
import datetime
from testcontainers.neo4j import Neo4jContainer
from pyspark.sql.types import TimestampType, DateType

with Neo4jContainer('neo4j:4.2') as neo4j_container:
    with neo4j_container.get_driver() as neo4j_driver:
        with neo4j_driver.session() as neo4j_session:
            spark = SparkSession.builder \
                .appName("Neo4jConnectorTests") \
                .master('local[*]') \
                .config(
                    "spark.jars",
                    "../../../target/neo4j-connector-apache-spark_2.12_3.0-4.0.0.jar"
                ) \
                .getOrCreate()

            def reset_db():
                neo4j_session.run("MATCH (n) DETACH DELETE n;")

            def init_test(query, parameters=None):
                reset_db()
                neo4j_session.run(query, parameters)
                return spark.read.format("org.neo4j.spark.DataSource") \
                    .option("url", neo4j_container.get_connection_url()) \
                    .option("authentication.type", "basic") \
                    .option("authentication.basic.username", "neo4j") \
                    .option("authentication.basic.password", "password") \
                    .option("labels", "Person") \
                    .load()

            def test_string():
                name = "Foobar"
                df = init_test("CREATE (p:Person {name: '" + name + "'})")

                assert name == df.select("name").collect()[0].name

            def test_int():
                age = 32
                df = init_test("CREATE (p:Person {age: " + str(age) + "})")

                assert age == df.select("age").collect()[0].age

            def test_double():
                score = 32.3
                df = init_test("CREATE (p:Person {score: " + str(score) + "})")

                assert score == df.select("score").collect()[0].score

            def test_boolean():
                df = init_test("CREATE (p:Person {boolean: true})")

                assert True == df.select("boolean").collect()[0].boolean

            # https://neo4j.com/docs/api/python-driver/current/temporal_types.html
            def test_time():
                time = datetime.time(12, 23, 0)
                df = init_test(
                    "CREATE (p:Person {myTime: localtime({hour:12, minute: 23, second: 0})})"
                )

                timeResult = df.select("myTime").collect()[0].myTime

                assert "local-time" == timeResult.type
                assert str(time) == timeResult.value

            def test_datetime():
                dtString = "2015-06-24T12:50:35+00:00"
                df = init_test(
                    "CREATE (p:Person {datetime: datetime('"+dtString+"')})")

                dt = datetime.datetime(
                    2015, 6, 24, 12, 50, 35, 0, datetime.timezone.utc)
                dtResult = df.select("datetime").collect()[
                    0].datetime.astimezone(datetime.timezone.utc)

                assert dt == dtResult

            def test_date():
                df = init_test("CREATE (p:Person {born: date('2009-10-10')})")

                dt = datetime.date(2009, 10, 10)
                dtResult = df.select("born").collect()[0].born

                assert dt == dtResult

            def test_point():
                df = init_test(
                    "CREATE (p:Person {location: point({x: 12.12, y: 13.13})})"
                )

                pointResult = df.select("location").collect()[0].location
                assert "point-2d" == pointResult[0]
                assert 7203 == pointResult[1]
                assert 12.12 == pointResult[2]
                assert 13.13 == pointResult[3]

            def test_point3d():
                df = init_test(
                    "CREATE (p:Person {location: point({x: 12.12, y: 13.13, z: 1})})"
                )

                pointResult = df.select("location").collect()[0].location
                assert "point-3d" == pointResult[0]
                assert 9157 == pointResult[1]
                assert 12.12 == pointResult[2]
                assert 13.13 == pointResult[3]
                assert 1.0 == pointResult[4]

            def test_geopoint():
                df = init_test(
                    "CREATE (p:Person {location: point({longitude: 12.12, latitude: 13.13})})"
                )

                pointResult = df.select("location").collect()[0].location
                assert "point-2d" == pointResult[0]
                assert 4326 == pointResult[1]
                assert 12.12 == pointResult[2]
                assert 13.13 == pointResult[3]

            def test_duration():
                df = init_test(
                    "CREATE (p:Person {range: duration({days: 14, hours:16, minutes: 12})})"
                )

                durationResult = df.select("range").collect()[0].range
                assert "duration" == durationResult[0]
                assert 0 == durationResult[1]
                assert 14 == durationResult[2]
                assert 58320 == durationResult[3]
                assert 0 == durationResult[4]

            def test_string_array():
                df = init_test("CREATE (p:Person {names: ['John', 'Doe']})")

                result = df.select("names").collect()[0].names
                assert "John" == result[0]
                assert "Doe" == result[1]

            def test_int_array():
                df = init_test("CREATE (p:Person {ages: [24, 56]})")

                result = df.select("ages").collect()[0].ages
                assert 24 == result[0]
                assert 56 == result[1]

            def test_double_array():
                df = init_test("CREATE (p:Person {scores: [24.11, 56.11]})")

                result = df.select("scores").collect()[0].scores
                assert 24.11 == result[0]
                assert 56.11 == result[1]

            def test_boolean_array():
                df = init_test("CREATE (p:Person {field: [true, false]})")

                result = df.select("field").collect()[0].field
                assert True == result[0]
                assert False == result[1]

            def test_time_array():
                df = init_test(
                    "CREATE (p:Person {result: [localtime({hour:11, minute: 23, second: 0}), localtime({hour:12, minute: 23, second: 0})]})"
                )

                timeResult = df.select("result").collect()[0].result

                assert "local-time" == timeResult[0].type
                assert str(datetime.time(11, 23, 0)) == timeResult[0].value

                assert "local-time" == timeResult[1].type
                assert str(datetime.time(12, 23, 0)) == timeResult[1].value

            def test_localdatetime_array():
                df = init_test(
                    "CREATE (p:Person {result: [localdatetime('2007-12-03T10:15:30'), localdatetime('2008-12-03T10:15:30')]})"
                )

                dt1 = datetime.datetime(2007, 12, 3, 10, 15, 30)
                dt2 = datetime.datetime(2008, 12, 3, 10, 15, 30)
                dtResult = df.select("result").collect()[0].result

                assert dt1 == dtResult[0]
                assert dt2 == dtResult[1]

            def test_datetime_array():
                df = init_test(
                    "CREATE (p:Person {result: [datetime('2007-12-03T10:15:30+00:00'), datetime('2008-12-03T10:15:30+00:00')]})"
                )

                dt1 = datetime.datetime(2007, 12, 3, 10, 15, 30, 0, datetime.timezone.utc)
                dt2 = datetime.datetime(2008, 12, 3, 10, 15, 30, 0, datetime.timezone.utc)
                dtResult = df.select("result").collect()[0].result

                assert dt1 == dtResult[0].astimezone(datetime.timezone.utc)
                assert dt2 == dtResult[1].astimezone(datetime.timezone.utc)

            def test_date_array():
                df = init_test(
                    "CREATE (p:Person {result: [date('2009-10-10'), date('2008-10-10')]})"
                )

                dt1 = datetime.date(2009, 10, 10)
                dt2 = datetime.date(2008, 10, 10)
                dtResult = df.select("result").collect()[0].result

                assert dt1 == dtResult[0]
                assert dt2 == dtResult[1]

            def test_point_array():
                df = init_test(
                    "CREATE (p:Person {location: [point({x: 12.12, y: 13.13}), point({x: 13.13, y: 14.14})]})"
                )

                pointResult = df.select("location").collect()[0].location
                assert "point-2d" == pointResult[0][0]
                assert 7203 == pointResult[0][1]
                assert 12.12 == pointResult[0][2]
                assert 13.13 == pointResult[0][3]

                assert "point-2d" == pointResult[1][0]
                assert 7203 == pointResult[1][1]
                assert 13.13 == pointResult[1][2]
                assert 14.14 == pointResult[1][3]

            def test_point3d_array():
                df = init_test(
                    "CREATE (p:Person {location: [point({x: 12.12, y: 13.13, z: 1}), point({x: 14.14, y: 15.15, z: 1})]})"
                )

                pointResult = df.select("location").collect()[0].location
                assert "point-3d" == pointResult[0][0]
                assert 9157 == pointResult[0][1]
                assert 12.12 == pointResult[0][2]
                assert 13.13 == pointResult[0][3]
                assert 1.0 == pointResult[0][4]

                assert "point-3d" == pointResult[1][0]
                assert 9157 == pointResult[1][1]
                assert 14.14 == pointResult[1][2]
                assert 15.15 == pointResult[1][3]
                assert 1.0 == pointResult[1][4]

            def test_geopoint_array():
                df = init_test(
                    "CREATE (p:Person {location: [point({longitude: 12.12, latitude: 13.13}), point({longitude: 14.14, latitude: 15.15})]})"
                )

                pointResult = df.select("location").collect()[0].location
                assert "point-2d" == pointResult[0][0]
                assert 4326 == pointResult[0][1]
                assert 12.12 == pointResult[0][2]
                assert 13.13 == pointResult[0][3]

                assert "point-2d" == pointResult[1][0]
                assert 4326 == pointResult[1][1]
                assert 14.14 == pointResult[1][2]
                assert 15.15 == pointResult[1][3]

            def test_duration_array():
                df = init_test(
                    "CREATE (p:Person {range: [duration({days: 14, hours:16, minutes: 12}), duration({days: 15, hours:16, minutes: 12})]})"
                )

                durationResult = df.select("range").collect()[0].range
                assert "duration" == durationResult[0][0]
                assert 0 == durationResult[0][1]
                assert 14 == durationResult[0][2]
                assert 58320 == durationResult[0][3]
                assert 0 == durationResult[0][4]

                assert "duration" == durationResult[1][0]
                assert 0 == durationResult[1][1]
                assert 15 == durationResult[1][2]
                assert 58320 == durationResult[1][3]
                assert 0 == durationResult[1][4]

            test_string()
            test_int()
            test_double()
            test_boolean()
            test_time()
            test_datetime()
            test_date()
            test_point()
            test_point3d()
            test_geopoint()
            test_duration()
            test_string_array()
            test_int_array()
            test_double_array()
            test_boolean_array()
            test_time_array()
            test_datetime_array()
            test_date_array()
            test_point_array()
            test_point3d_array()
            test_geopoint_array()
            test_duration_array()
