package com.github.alekkol.trino.wrike;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestEmbedded extends AbstractTestQueryFramework {
    private static final Set<String> SHORTCUTS = Set.of("token", "port", "presto");

    static {
        for (String shortcut : SHORTCUTS) {
            String property = System.getProperty(shortcut);
            if (property != null) {
                System.setProperty("com.github.alekkol.trino.wrike." + shortcut, property);
            }
        }
    }

    private final CountDownLatch latch = new CountDownLatch(1);

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        Session defaultSession = testSessionBuilder()
                .setCatalog("wrike")
                .setSchema("rest")
                .build();

        DistributedQueryRunner.Builder<?> runnerBuilder = DistributedQueryRunner.builder(defaultSession)
                .addExtraProperty("http-server.http.port", System.getProperty("com.github.alekkol.trino.wrike.port"))
                .setNodeCount(1);
        if (Boolean.getBoolean("com.github.alekkol.trino.wrike.presto")) {
            runnerBuilder.addExtraProperty("protocol.v1.alternate-header-name", "Presto");
        }
        QueryRunner queryRunner = runnerBuilder.build();
        queryRunner.installPlugin(new WrikePlugin());

        queryRunner.createCatalog(
                "wrike",
                "wrike_rest",
                Map.of());

        return queryRunner;
    }

    @Test
    public void run() throws InterruptedException {
        Runtime.getRuntime().addShutdownHook(new Thread(latch::countDown));
        latch.await();
    }
}
