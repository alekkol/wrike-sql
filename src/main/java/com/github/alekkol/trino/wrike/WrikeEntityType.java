package com.github.alekkol.trino.wrike;

import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;

import java.util.List;
import java.util.stream.Stream;

import static io.trino.spi.type.VarcharType.VARCHAR;

public enum WrikeEntityType {
    TASK("tasks",
            "/tasks?fields=%5B%22responsibleIds%22%5D", // todo hack
            textScalar("id"),
            textScalar("title"),
            textScalar("status"),
            ColumnMetadata.builder()
                    .setName("responsibleIds")
                    .setType(new ArrayType(VARCHAR))
                    .setNullable(true)
                    .build()),
    CONTACT("contacts",
            "/contacts",
            textScalar("id"),
            textScalar("firstName"),
            textScalar("lastName"));

    private static ColumnMetadata textScalar(String id) {
        return ColumnMetadata.builder()
                .setName(id)
                .setType(VARCHAR)
                .setNullable(true)
                .build();
    }

    private final String tableName;
    private final String endpoint;
    private final List<ColumnMetadata> columns;

    WrikeEntityType(String tableName, String endpoint, ColumnMetadata... columns) {
        this.tableName = tableName;
        this.endpoint = endpoint;
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

    public String getEndpoint() {
        return endpoint;
    }

    public List<ColumnMetadata> getColumns() {
        return columns;
    }
}
