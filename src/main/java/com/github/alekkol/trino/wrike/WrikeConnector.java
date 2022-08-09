package com.github.alekkol.trino.wrike;

import io.airlift.slice.Slice;
import io.trino.spi.NodeManager;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.transaction.IsolationLevel;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

public class WrikeConnector implements Connector {
    private static final String SCHEMA = "rest";

    private final NodeManager nodeManager;

    public WrikeConnector(NodeManager nodeManager) {
        this.nodeManager = Objects.requireNonNull(nodeManager);
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        return WrikeConnectorTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return new WrikeConnectorSplitManager(nodeManager);
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return new WrikeConnectorRecordSetProvider();
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider() {
        return new ConnectorPageSinkProvider() {
            @Override
            public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle) {
                return new ConnectorPageSink() {
                    @Override
                    public CompletableFuture<?> appendPage(Page page) {
                        return null;
                    }

                    @Override
                    public CompletableFuture<Collection<Slice>> finish() {
                        return null;
                    }

                    @Override
                    public void abort() {

                    }
                };
            }

            @Override
            public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle) {
                return new ConnectorPageSink() {
                    @Override
                    public CompletableFuture<?> appendPage(Page page) {
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public CompletableFuture<Collection<Slice>> finish() {
                        return CompletableFuture.completedFuture(List.of());
                    }

                    @Override
                    public void abort() {
                    }
                };
            }
        };
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
            public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
                WrikeTableHandle wrikeTableHandle = (WrikeTableHandle) table;

                WrikeEntityType entityType = wrikeTableHandle.entityType();
                return new ConnectorTableMetadata(
                        SchemaTableName.schemaTableName(SCHEMA, entityType.getTableName()),
                        entityType.getColumns());
            }

            @Override
            public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
                WrikeTableHandle wrikeTableHandle = (WrikeTableHandle) tableHandle;

                return wrikeTableHandle.entityType().getColumns().stream()
                        .collect(toMap(ColumnMetadata::getName, WrikeColumnHandle::new));
            }

            @Override
            public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
                WrikeColumnHandle wrikeColumnHandle = (WrikeColumnHandle) columnHandle;

                return wrikeColumnHandle.toColumnMetadata();
            }

            @Override
            public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode) {
                return new WrikeInsertTableHandle();
            }

            @Override
            public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics) {
                return Optional.empty();
            }
        };
    }
}
