package com.github.alekkol.trino.wrike;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

public class WrikeConnector implements Connector {
    private static final String SCHEMA = "rest";

    @Override
    public ConnectorSplitManager getSplitManager() {
        return WrikeConnectorSplitManager.INSTANCE;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return new WrikeConnectorRecordSetProvider();
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
        return new ConnectorMetadata() {
            @Override
            public List<String> listSchemaNames(ConnectorSession session) {
                return List.of(SCHEMA);
            }

            @Override
            public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
                return Stream.of(WrikeEntityType.values())
                        .map(entityType -> new SchemaTableName(SCHEMA, entityType.name()))
                        .toList();
            }

            @Override
            public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
                return new WrikeTableHandle(WrikeEntityType.fromTableName(tableName.getTableName()));
            }

            @Override
            public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
                WrikeTableHandle wrikeTableHandle = (WrikeTableHandle) tableHandle;

                return wrikeTableHandle.entityType().getColumns().stream()
                        // todo hardcoded default
                        .collect(toMap(Function.identity(), column -> new WrikeColumnHandle(column, VarcharType.VARCHAR)));
            }

            @Override
            public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
                WrikeColumnHandle wrikeColumnHandle = (WrikeColumnHandle) columnHandle;

                return ColumnMetadata.builder()
                        .setName(wrikeColumnHandle.name())
                        .setType(wrikeColumnHandle.type())
                        // todo hardcoded default
                        .setNullable(false)
                        .build();
            }
        };
    }
}
