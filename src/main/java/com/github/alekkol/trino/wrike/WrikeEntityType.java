package com.github.alekkol.trino.wrike;

import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.github.alekkol.trino.wrike.WrikeBooleanRestColumn.bool;
import static com.github.alekkol.trino.wrike.WrikeNestedTextArrayRestColumn.nestedTextArray;
import static com.github.alekkol.trino.wrike.WrikeTextArrayRestColumn.textArray;
import static com.github.alekkol.trino.wrike.WrikeTextRestColumn.primaryKey;
import static com.github.alekkol.trino.wrike.WrikeTextRestColumn.text;
import static com.github.alekkol.trino.wrike.WrikeTimestampRestColumn.timestamp;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public enum WrikeEntityType {
    TASK("tasks",
            "/tasks?fields=%5B%22responsibleIds%22%2C%22authorIds%22%2C%22superTaskIds%22%2C%22subTaskIds%22%5D",
            "/tasks",
            List.of(primaryKey("id"), text("title").writable(),
                    text("status").writable(), text("customStatus", "customStatusId"),
                    timestamp("createdDate"), timestamp("updatedDate"),
                    textArray("authorIds"), textArray("responsibleIds"),
                    textArray("superTaskIds"), textArray("subTaskIds"),
                    text("permalink"))),
    FOLDER("folders",
            "/folders",
            List.of(primaryKey("id"), text("title").writable(),
                    text("scope"), textArray("childIds"))),
    CONTACT("contacts",
            "/contacts",
            List.of(primaryKey("id"),
                    text("firstName").writable(), text("lastName").writable(),
                    bool("deleted"), text("type"))),
    COMMENT("comments",
            "/comments",
            "/tasks/${taskId}/comments",
            "/comments",
            List.of(primaryKey("id"), text("authorId"),
                    text("text").writable(), timestamp("createdDate"),
                    text("taskId"))),
    WORKFLOW("workflows",
            "/workflows",
            "/workflows",
            List.of(primaryKey("id"), text("name").writable(),
                    bool("standard"), text("group"),
                    nestedTextArray("customStatusIds", "customStatuses", "id"))),
    CUSTOM_STATUS("custom_statuses",
            "/customstatuses",
            List.of(primaryKey("id"), text("name").writable(),
                    bool("standard"), text("group")));

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

    WrikeEntityType(String tableName, String endpoint, List<WrikeRestColumn> columns) {
        this(tableName, endpoint, endpoint, endpoint, columns);
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

    public List<WrikeRestColumn> getColumns() {
        return columns.values().asList();
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
