package com.github.alekkol.trino.wrike;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.*;

public class WrikeConnectorTest extends AbstractTestQueryFramework {
    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        Session defaultSession = testSessionBuilder()
                .setCatalog("wrike")
                .setSchema("rest")
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .setNodeCount(1)
                .build();
        queryRunner.installPlugin(new WrikePlugin());

        queryRunner.createCatalog(
                "wrike",
                "wrike",
                Map.of());

        return queryRunner;
    }

    @Test
    public void testShowQueries() {
        assertQuerySucceeds("SHOW SCHEMAS;");
        assertQuerySucceeds("SHOW TABLE FROM SCHEMA rest;");
    }
}