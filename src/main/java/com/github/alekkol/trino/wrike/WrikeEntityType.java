package com.github.alekkol.trino.wrike;

import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static io.trino.spi.type.VarcharType.VARCHAR;

public enum WrikeEntityType {
    TASK("tasks",
            "/tasks",
            ColumnMetadata.builder()
                    .setName("id")
                    .setType(VARCHAR)
                    .setNullable(false)
                    .build(),
            ColumnMetadata.builder()
                    .setName("title")
                    .setType(VARCHAR)
                    .setNullable(false)
                    .build(),
            ColumnMetadata.builder()
                    .setName("status")
                    .setType(VARCHAR)
                    .setNullable(false)
                    .build()),
    CONTACT("contacts",
            "/contacts",
            ColumnMetadata.builder()
                    .setName("id")
                    .setType(VARCHAR)
                    .setNullable(false)
                    .build(),
            ColumnMetadata.builder()
                    .setName("firstName")
                    .setType(VARCHAR)
                    .setNullable(false)
                    .build(),
            ColumnMetadata.builder()
                    .setName("lastName")
                    .setType(VARCHAR)
                    .setNullable(false)
                    .build());

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
