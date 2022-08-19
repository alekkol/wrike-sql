package com.github.alekkol.trino.wrike;

import io.airlift.slice.Slice;
import io.trino.spi.NodeManager;
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
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.transaction.IsolationLevel;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
                throw new UnsupportedOperationException("DDL not supported");
            }

            @Override
            public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle) {
                WrikeInsertTableHandle wrikeInsertTableHandle = (WrikeInsertTableHandle) insertTableHandle;

                return new WrikePageSink(wrikeInsertTableHandle.entityType());
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
                return new WrikeTableHandle(WrikeEntityType.fromTableName(tableName.getTableName()), Optional.empty());
            }

            @Override
            public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
                WrikeTableHandle wrikeTableHandle = (WrikeTableHandle) table;

                WrikeEntityType entityType = wrikeTableHandle.entityType();
                return new ConnectorTableMetadata(
                        SchemaTableName.schemaTableName(SCHEMA, entityType.getTableName()),
                        entityType.getColumns().stream()
                                .map(WrikeRestColumn::metadata)
                                .toList());
            }

            @Override
            public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
                WrikeTableHandle wrikeTableHandle = (WrikeTableHandle) tableHandle;

                return wrikeTableHandle.entityType().getColumns().stream()
                        .collect(toMap(
                                column -> column.metadata().getName(),
                                column -> new WrikeColumnHandle(column.metadata().getName(), column.isPrimaryKey())
                        ));
            }

            @Override
            public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
                WrikeTableHandle wrikeTableHandle = (WrikeTableHandle) tableHandle;
                WrikeColumnHandle wrikeColumnHandle = (WrikeColumnHandle) columnHandle;

                return wrikeTableHandle.entityType().getColumn(wrikeColumnHandle.name()).metadata();
            }

            @Override
            public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode) {
                WrikeTableHandle wrikeTableHandle = (WrikeTableHandle) tableHandle;

                return new WrikeInsertTableHandle(wrikeTableHandle.entityType());
            }

            @Override
            public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics) {
                return Optional.empty();
            }

            @Override
            public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint) {
                WrikeTableHandle wrikeTableHandle = (WrikeTableHandle) handle;

                Map<ColumnHandle, Domain> columnHandleToDomainMap = constraint.getSummary().getDomains().orElseThrow();
                Map<ColumnHandle, Domain> pkColumnDomains = new LinkedHashMap<>();
                Map<ColumnHandle, Domain> otherColumnDomains = new LinkedHashMap<>();
                columnHandleToDomainMap.forEach((columnHandle, domain) -> {
                    // push down filter by PK with single value (e.g. id = 'QWERTY')
                    if (columnHandle instanceof WrikeColumnHandle wrikeColumnHandle
                            && wrikeColumnHandle.primaryKey()
                            && domain.isSingleValue()) {
                        pkColumnDomains.put(columnHandle, domain);
                    } else {
                        otherColumnDomains.put(columnHandle, domain);
                    }
                });

                ColumnMetadata pkMetadata = wrikeTableHandle.entityType().getPkColumn().metadata();
                WrikeColumnHandle pkColumn = new WrikeColumnHandle(pkMetadata.getName(), true);
                TupleDomain<ColumnHandle> oldDomain = wrikeTableHandle.id()
                        .map(id -> TupleDomain.withColumnDomains(Map.of(
                                (ColumnHandle) pkColumn,
                                Domain.singleValue(pkMetadata.getType(), id))))
                        .orElse(TupleDomain.all());
                TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(TupleDomain.withColumnDomains(pkColumnDomains));

                if (oldDomain.equals(newDomain)) {
                    return Optional.empty();
                } else {
                    return Optional.of(new ConstraintApplicationResult<>(
                            new WrikeTableHandle(wrikeTableHandle.entityType(), newDomain.getDomains()
                                    .map(domains -> domains.get(pkColumn))
                                    .map(Domain::getSingleValue)
                                    .map(Slice.class::cast)
                                    .map(Slice::toStringUtf8)),
                            TupleDomain.withColumnDomains(otherColumnDomains),
                            false));
                }
            }
        };
    }
}
