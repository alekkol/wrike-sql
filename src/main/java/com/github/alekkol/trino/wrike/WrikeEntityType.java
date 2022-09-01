package com.github.alekkol.trino.wrike;

import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.github.alekkol.trino.wrike.WrikeBooleanRestColumn.bool;
import static com.github.alekkol.trino.wrike.WrikeTextArrayRestColumn.textArray;
import static com.github.alekkol.trino.wrike.WrikeTextRestColumn.primaryKey;
import static com.github.alekkol.trino.wrike.WrikeTextRestColumn.text;
import static com.github.alekkol.trino.wrike.WrikeTimestampRestColumn.timestamp;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public enum WrikeEntityType {
    TASK("tasks",
            "/tasks?fields=%5B%22responsibleIds%22%2C%22authorIds%22%2C%22superTaskIds%22%2C%22subTaskIds%22%5D",
            "/tasks",
            List.of(primaryKey("id"), text("title"),
                    text("status"), timestamp("createdDate"),
                    textArray("authorIds"), textArray("responsibleIds"),
                    textArray("superTaskIds"), textArray("subTaskIds"),
                    text("permalink"))),
    FOLDER("folders",
            "/folders",
            "/folders",
            List.of(primaryKey("id"), text("title"),
                    text("scope"), textArray("childIds"))),
    CONTACT("contacts",
            "/contacts",
            "/contacts",
            List.of(primaryKey("id"),
                    text("firstName"), text("lastName"),
                    bool("deleted"), text("type"))),
    COMMENT("comments",
            "/comments",
            "/tasks/${taskId}/comments",
            "/comments",
            List.of(primaryKey("id"), text("authorId"),
                    text("text"), timestamp("createdDate"),
                    text("taskId")));

    private final String tableName;
    private final String selectAllEndpoint;
    private final String insertEndpoint;
    private final String baseEndpoint;
    private final ImmutableMap<String, WrikeRestColumn> columns;

    WrikeEntityType(String tableName, String selectAllEndpoint, String insertEndpoint, String baseEndpoint, List<WrikeRestColumn> columns) {
        this.tableName = tableName;
        this.selectAllEndpoint = selectAllEndpoint;
        this.insertEndpoint = insertEndpoint;
        this.baseEndpoint = baseEndpoint;
        this.columns = columns.stream()
                .collect(toImmutableMap(column -> column.metadata().getName().toLowerCase(), Function.identity()));
    }

    WrikeEntityType(String tableName, String selectAllEndpoint, String baseEndpoint, List<WrikeRestColumn> columns) {
        this(tableName, selectAllEndpoint, baseEndpoint, baseEndpoint, columns);
    }

    public static WrikeEntityType fromTableName(String tableName) {
        return Stream.of(values())
                .filter(entityType -> tableName.equalsIgnoreCase(entityType.tableName))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("No entity for table name: " + tableName));
    }

    public String getTableName() {
        return tableName;
    }

    public String getSelectAllEndpoint() {
        return selectAllEndpoint;
    }

    public String getInsertEndpoint() {
        return insertEndpoint;
    }

    public String getBaseEndpoint() {
        return baseEndpoint;
    }

    public Collection<WrikeRestColumn> getColumns() {
        return columns.values();
    }

    public WrikeRestColumn getPkColumn() {
        return getColumns().stream()
                .filter(WrikeRestColumn::isPrimaryKey)
                .findAny()
                .orElseThrow(() -> new IllegalStateException("No PK for entity type: " + this));
    }

    public WrikeRestColumn getColumn(String name) {
        WrikeRestColumn found = columns.get(name.toLowerCase());
        if (found == null) {
            throw new IllegalStateException("No '%s' column in table '%s'".formatted(name, tableName));
        }
        return found;
    }
}
