package com.github.alekkol.trino.wrike;

import java.util.Set;
import java.util.stream.Stream;

public enum WrikeEntityType {
    TASK("tasks",
            "/tasks",
            "id", "title", "status"),
    CONTACT("contacts",
            "/contacts",
            "id", "firstName", "lastName");

    private final String tableName;
    private final String endpoint;
    private final Set<String> columns;

    WrikeEntityType(String tableName, String endpoint, String... columns) {
        this.tableName = tableName;
        this.endpoint = endpoint;
        this.columns = Set.of(columns);
    }

    public static WrikeEntityType fromTableName(String tableName) {
        return Stream.of(values())
                .filter(entityType -> tableName.equalsIgnoreCase(entityType.tableName))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("No entity for table name: " + tableName));
    }

    public String getEndpoint() {
        return endpoint;
    }

    public Set<String> getColumns() {
        return columns;
    }
}
