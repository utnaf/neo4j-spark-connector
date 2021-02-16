package org.neo4j.spark.service;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Config;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.spark.util.DriverCache;
import org.neo4j.spark.util.Neo4jDriverOptions;
import org.neo4j.spark.util.Neo4jOptions;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.testcontainers.shaded.com.google.common.io.BaseEncoding;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.times;

@PrepareForTest({GraphDatabase.class})
@RunWith(PowerMockRunner.class)
public class AuthenticationTest {

    @Test
    public void testLdapConnectionToken() {
        Map<String, String> options = new HashMap<>();
        options.put("url", "bolt://localhost:7687");
        options.put("authentication.type", "custom");
        options.put(
                "authentication.custom.credentials",
                BaseEncoding.base64().encode("user:password".getBytes())
        );
        options.put("labels", "Person");

        ArgumentCaptor<AuthToken> argumentCaptor = ArgumentCaptor.forClass(AuthToken.class);
        Neo4jOptions neo4jOptions = new Neo4jOptions(options);
        Neo4jDriverOptions neo4jDriverOptions = neo4jOptions.connection();
        DriverCache driverCache = new DriverCache(neo4jDriverOptions, "jobId");

        PowerMockito.mockStatic(GraphDatabase.class);

        driverCache.getOrCreate();

        PowerMockito.verifyStatic(GraphDatabase.class, times(1));
        GraphDatabase.driver(
                anyString(),
                argumentCaptor.capture(),
                any(Config.class)
        );

        assertEquals(neo4jDriverOptions.toNeo4jAuth(), argumentCaptor.getValue());
    }


}
