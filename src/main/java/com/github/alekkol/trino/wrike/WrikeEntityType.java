package com.github.alekkol.trino.wrike;

import java.util.List;
import java.util.stream.Stream;

public enum WrikeEntityType {
    TASK("tasks",
            "/tasks?fields=%5B%22responsibleIds%22%5D", // todo hack
            "/tasks",
            new WrikeTextRestColumn("id"),
            new WrikeTextRestColumn("title"),
            new WrikeTextRestColumn("status"),
            new WrikeTextArrayRestColumn("responsibleIds")),
    CONTACT("contacts",
            "/contacts",
            "/dummy",
            new WrikeTextRestColumn("id"),
            new WrikeTextRestColumn("firstName"),
            new WrikeTextRestColumn("lastName"));

    private final String tableName;
    private final String selectEndpoint;
    private final String insertEndpoint;
    private final List<WrikeRestColumn> columns;

    WrikeEntityType(String tableName, String selectEndpoint, String insertEndpoint, WrikeRestColumn... columns) {
        this.tableName = tableName;
        this.selectEndpoint = selectEndpoint;
        this.insertEndpoint = insertEndpoint;
        this.columns = List.of(columns);
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

    public String getSelectEndpoint() {
        return selectEndpoint;
    }

    public String getInsertEndpoint() {
        return insertEndpoint;
    }

    public List<WrikeRestColumn> getColumns() {
        return columns;
    }

    public WrikeRestColumn getColumn(String name) {
        return columns.stream()
                .filter(column -> name.equalsIgnoreCase(column.metadata().getName()))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("No '%s' column in table '%s'"
                        .formatted(name, tableName)));
    }
}
