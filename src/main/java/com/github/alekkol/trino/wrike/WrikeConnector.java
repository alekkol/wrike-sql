package com.github.alekkol.trino.wrike;

import io.airlift.slice.Slice;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.*;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.transaction.IsolationLevel;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.spi.type.VarcharType.VARCHAR;
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
    public ConnectorPageSourceProvider getPageSourceProvider() {
        return new WrikePageSourceProvider();
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider() {
        return new ConnectorPageSinkProvider() {
            @Override
            public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, ConnectorPageSinkId pageSinkId) {
                throw new UnsupportedOperationException("DDL not supported");
            }

            @Override
            public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, ConnectorPageSinkId pageSinkId) {
                WrikeInsertTableHandle wrikeInsertTableHandle = (WrikeInsertTableHandle) insertTableHandle;

                return new WrikePageSink(wrikeInsertTableHandle.entityType());
            }

            @Override
            public ConnectorMergeSink createMergeSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorMergeTableHandle mergeHandle, ConnectorPageSinkId pageSinkId) {
                WrikeMergeTableHandle wrikeMergeHandle = (WrikeMergeTableHandle) mergeHandle;
                WrikeTableHandle tableHandle = wrikeMergeHandle.tableHandle();
                return new WrikeMergeSink(tableHandle.entityType());
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
                        .map(entityType -> new SchemaTableName(SCHEMA, entityType.getTableName()))
                        .toList();
            }

            @Override
            public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
                return prefix.getTable()
                        .map(WrikeEntityType::fromTableName)
                        .map(entityType -> TableColumnsMetadata.forTable(
                                SchemaTableName.schemaTableName(SCHEMA, entityType.getTableName()),
                                entityType.getColumns().stream()
                                        .map(WrikeRestColumn::metadata)
                                        .toList()))
                        .stream()
                        .iterator();
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

                if (wrikeColumnHandle.isRowId()) {
                    return new ColumnMetadata(wrikeColumnHandle.name(), VARCHAR);
                } else {
                    return wrikeTableHandle.entityType().getColumn(wrikeColumnHandle.name()).metadata();
                }
            }

            @Override
            public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle) {
                return WrikeColumnHandle.rowId();
            }

            @Override
            public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle) {
                return RowChangeParadigm.CHANGE_ONLY_UPDATED_COLUMNS;
            }

            @Override
            public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle, RetryMode retryMode) {
                return new WrikeMergeTableHandle((WrikeTableHandle) tableHandle);
            }

            @Override
            public void finishMerge(ConnectorSession session, ConnectorMergeTableHandle mergeTableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics) {
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
            public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session,
                                                                                           ConnectorTableHandle handle,
                                                                                           Constraint constraint) {
                WrikeTableHandle wrikeTableHandle = (WrikeTableHandle) handle;

                Map<ColumnHandle, Domain> columnHandleToDomainMap = constraint.getSummary().getDomains().orElseThrow();
                Map<ColumnHandle, Domain> pkColumnDomains = new LinkedHashMap<>();
                Map<ColumnHandle, Domain> otherColumnDomains = new LinkedHashMap<>();
                columnHandleToDomainMap.forEach((columnHandle, domain) -> {
                    // push down filter by PK with one or several values
                    //     id = 'QWERTY'
                    //     id IN ('QWERTY', 'DVORAK')
                    if (columnHandle instanceof WrikeColumnHandle wrikeColumnHandle
                            && wrikeColumnHandle.primaryKey()) {
                        pkColumnDomains.put(columnHandle, domain);
                    } else {
                        otherColumnDomains.put(columnHandle, domain);
                    }
                });

                ColumnMetadata pkMetadata = wrikeTableHandle.entityType().getPkColumn().metadata();
                WrikeColumnHandle pkColumn = new WrikeColumnHandle(pkMetadata.getName(), true);
                TupleDomain<ColumnHandle> oldDomain = wrikeTableHandle.ids()
                        .map(ids -> TupleDomain.withColumnDomains(Map.of(
                                (ColumnHandle) pkColumn,
                                Domain.multipleValues(pkMetadata.getType(), ids))))
                        .orElse(TupleDomain.all());
                TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(TupleDomain.withColumnDomains(pkColumnDomains));

                if (oldDomain.equals(newDomain)) {
                    return Optional.empty();
                } else {
                    return Optional.of(new ConstraintApplicationResult<>(
                            new WrikeTableHandle(
                                    wrikeTableHandle.entityType(),
                                    newDomain.getDomains()
                                            .map(domains -> domains.get(pkColumn))
                                            .map(Domain::getValues)
                                            .map(ValueSet::getDiscreteSet)
                                            .map(discreteSet -> discreteSet.stream()
                                                    .map(Slice.class::cast)
                                                    .map(Slice::toStringUtf8)
                                                    .toList())),
                            TupleDomain.withColumnDomains(otherColumnDomains),
                            false));
                }
            }

            @Override
            public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle) {
                WrikeTableHandle wrikeTableHandle = (WrikeTableHandle) tableHandle;
                WrikeRestColumn pkColumn = wrikeTableHandle.entityType().getPkColumn();
                WrikeColumnHandle pkColumnHandle = new WrikeColumnHandle(pkColumn.metadata().getName(), pkColumn.isPrimaryKey());
                return new TableStatistics.Builder()
                        .setRowCount(Estimate.unknown())
                        .setColumnStatistics(pkColumnHandle, ColumnStatistics.builder()
                                .setNullsFraction(Estimate.zero())
                                .setDistinctValuesCount(Estimate.of(Math.pow(2, 31)))
                                .build())
                        .build();
            }
        };
    }
}
