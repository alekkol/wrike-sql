package com.github.alekkol.trino.wrike;

import java.util.List;
import java.util.stream.Stream;

import static com.github.alekkol.trino.wrike.WrikeTextArrayRestColumn.textArray;
import static com.github.alekkol.trino.wrike.WrikeTextRestColumn.primaryKey;
import static com.github.alekkol.trino.wrike.WrikeTextRestColumn.text;
import static com.github.alekkol.trino.wrike.WrikeTimestampRestColumn.timestamp;

public enum WrikeEntityType {
    TASK("tasks",
            "/tasks?fields=%5B%22responsibleIds%22%5D", // todo hack
            "/tasks",
            List.of(primaryKey("id"), text("title"),
                    text("status"), timestamp("createdDate"),
                    textArray("responsibleIds"))),
    CONTACT("contacts",
            "/contacts",
            "/contacts",
            List.of(primaryKey("id"), text("firstName"), text("lastName")));

    private final String tableName;
    private final String selectAllEndpoint;
    private final String baseEndpoint;
    private final List<WrikeRestColumn> columns;

    WrikeEntityType(String tableName, String selectAllEndpoint, String baseEndpoint, List<WrikeRestColumn> columns) {
        this.tableName = tableName;
        this.selectAllEndpoint = selectAllEndpoint;
        this.baseEndpoint = baseEndpoint;
        this.columns = columns;
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

    public String getBaseEndpoint() {
        return baseEndpoint;
    }

    public List<WrikeRestColumn> getColumns() {
        return columns;
    }

    public WrikeRestColumn getPkColumn() {
        return columns.stream()
                .filter(WrikeRestColumn::isPrimaryKey)
                .findAny()
                .orElseThrow(() -> new IllegalStateException("No PK for entity type: " + this));
    }

    public WrikeRestColumn getColumn(String name) {
        return columns.stream()
                .filter(column -> name.equalsIgnoreCase(column.metadata().getName()))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("No '%s' column in table '%s'"
                        .formatted(name, tableName)));
    }
}
