package com.github.alekkol.trino.wrike;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestEmbedded extends AbstractTestQueryFramework {
    private final CountDownLatch latch = new CountDownLatch(1);

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        Session defaultSession = testSessionBuilder()
                .setCatalog("wrike")
                .setSchema("rest")
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .addExtraProperty("http-server.http.port", System.getProperty("com.github.alekkol.trino.wrike.port", "6565"))
                .setNodeCount(1)
                .build();
        queryRunner.installPlugin(new WrikePlugin());

        queryRunner.createCatalog(
                "wrike",
                "wrike-rest",
                Map.of());

        return queryRunner;
    }

    @Test
    public void run() throws InterruptedException {
        Runtime.getRuntime().addShutdownHook(new Thread(latch::countDown));
        latch.await();
    }
}
