package com.github.alekkol.trino.wrike;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestWrikeConnector extends AbstractTestQueryFramework {
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
                "wrike-rest",
                Map.of());

        return queryRunner;
    }

    @Test
    public void testShowQueries() {
        assertQuerySucceeds("SHOW SCHEMAS");
        assertQuerySucceeds("SHOW TABLES FROM rest");
    }

    @Test
    public void testShowColumns() {
        assertThat(computeActual("SHOW COLUMNS FROM rest.tasks")).isNotEmpty();
        assertThat(computeActual("SHOW COLUMNS FROM rest.contacts")).isNotEmpty();
    }

    @Test
    public void testSelectAll() {
        assertQuerySucceeds("SELECT * FROM wrike.rest.tasks");
        assertQuerySucceeds("SELECT * FROM wrike.rest.contacts");
    }

    @Test
    public void testSelectTaskById() {
        String taskId = getQueryRunner().execute("SELECT id FROM wrike.rest.tasks LIMIT 1")
                .getOnlyValue()
                .toString();
        assertThat(computeActual("SELECT id FROM wrike.rest.tasks WHERE id = '%s'".formatted(taskId)).getOnlyValue())
                .isEqualTo(taskId);
    }

    @Test
    public void testSelectContactById() {
        String contactId = getQueryRunner().execute("SELECT id FROM wrike.rest.contacts LIMIT 1")
                .getOnlyValue()
                .toString();
        assertThat(computeActual("SELECT id FROM wrike.rest.contacts WHERE id = '%s'".formatted(contactId)).getOnlyValue())
                .isEqualTo(contactId);
    }

    @Test
    public void testSelectCount() {
        assertQuerySucceeds("SELECT COUNT(*) FROM wrike.rest.tasks");
        assertQuerySucceeds("SELECT COUNT(*) FROM wrike.rest.contacts");
    }

    @Test
    public void testInsertTask() {
        assertQuerySucceeds("INSERT INTO wrike.rest.tasks(title) VALUES('hello')");
    }

    @Test
    public void testInsertComment() {
        assertQuerySucceeds("INSERT INTO wrike.rest.tasks(title) VALUES('to be updated')");
        String taskId = getQueryRunner().execute("SELECT id FROM wrike.rest.tasks ORDER BY createddate DESC LIMIT 1")
                .getOnlyValue()
                .toString();
        assertQuerySucceeds("INSERT INTO wrike.rest.comments(taskId, text) VALUES('%s', 'hello from test')"
                .formatted(taskId));
    }

    @Test
    public void testUpdateTask() {
        assertQuerySucceeds("INSERT INTO wrike.rest.tasks(title) VALUES('to be updated')");
        String taskId = getQueryRunner().execute("SELECT id FROM wrike.rest.tasks ORDER BY createddate DESC LIMIT 1")
                .getOnlyValue()
                .toString();
        assertQuerySucceeds("UPDATE wrike.rest.tasks SET title = 'hello', status = 'Completed' WHERE id = '%s'".formatted(taskId));
    }

    @Test
    public void testDeleteTaskById() {
        String taskId = getQueryRunner().execute("SELECT id FROM wrike.rest.tasks LIMIT 1")
                .getOnlyValue()
                .toString();
        assertQuerySucceeds("DELETE FROM wrike.rest.tasks WHERE id = '%s'".formatted(taskId));
    }
}