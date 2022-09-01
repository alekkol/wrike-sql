package com.github.alekkol.trino.wrike;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.UpdatablePageSource;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class WrikePageSourceProvider implements ConnectorPageSourceProvider {
    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction,
                                                ConnectorSession session,
                                                ConnectorSplit split,
                                                ConnectorTableHandle table,
                                                List<ColumnHandle> columns,
                                                DynamicFilter dynamicFilter) {
        WrikeTableHandle wrikeTableHandle = (WrikeTableHandle) table;
        WrikeEntityType wrikeEntityType = wrikeTableHandle.entityType();
        List<WrikeRestColumn> wrikeRestColumns = Stream
                .concat(columns.stream(), wrikeTableHandle.updatedColumns().stream())
                .map(WrikeColumnHandle.class::cast)
                .map(column -> {
                    if (column.isRowId()) {
                        return wrikeEntityType.getPkColumn();
                    } else {
                        return wrikeEntityType.getColumn(column.name());
                    }
                })
                .toList();
        List<Type> types = wrikeRestColumns.stream()
                .map(WrikeRestColumn::metadata)
                .map(ColumnMetadata::getType)
                .toList();

        record NextPageMark(String token, boolean terminal) {
        }

        return new UpdatablePageSource() {
            final long startNanoTime = System.nanoTime();
            final AtomicLong completedBytes = new AtomicLong();
            NextPageMark nextPageMark = new NextPageMark(null, false);
            private boolean closed;

            @Override
            public CompletableFuture<Collection<Slice>> finish() {
                return CompletableFuture.completedFuture(List.of());
            }

            @Override
            public long getCompletedBytes() {
                return completedBytes.get();
            }

            @Override
            public long getReadTimeNanos() {
                return System.nanoTime() - startNanoTime;
            }

            @Override
            public boolean isFinished() {
                return closed || nextPageMark.terminal();
            }

            @Override
            public Page getNextPage() {
                if (isFinished()) {
                    return null;
                }

                final StringBuilder uriBuilder = new StringBuilder();
                if (nextPageMark.token() != null) {
                    uriBuilder.append(wrikeEntityType.getBaseEndpoint()).append("?nextPageToken=").append(nextPageMark.token());
                } else {
                    wrikeTableHandle.ids().ifPresentOrElse(
                            ids -> uriBuilder.append(wrikeEntityType.getBaseEndpoint()).append("/").append(String.join(",", ids)),
                            () -> uriBuilder.append(wrikeEntityType.getSelectAllEndpoint()));
                }

                String response = Http.sync(uriBuilder.toString(), HttpRequest.Builder::GET);
                ObjectMapper objectMapper = new ObjectMapper();
                Map<String, Object> result;
                try {
                    //noinspection unchecked
                    result = objectMapper.readValue(response, Map.class);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                @SuppressWarnings("unchecked")
                List<Map<String, Object>> data = (List<Map<String, Object>>) requireNonNull(result.get("data"), "No 'data' in response");
                Object nextPageToken = result.get("nextPageToken");
                nextPageMark = new NextPageMark(Objects.toString(nextPageToken), nextPageToken == null);

                PageBuilder pageBuilder = PageBuilder.withMaxPageSize(100 * 1024 * 1024, types);
                for (Map<String, Object> row : data) {
                    pageBuilder.declarePosition();
                    for (int column = 0; column < types.size(); column++) {
                        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(column);
                        WrikeRestColumn restColumn = wrikeRestColumns.get(column);
                        restColumn.toBlock(row, blockBuilder);
                    }
                }
                completedBytes.addAndGet(pageBuilder.getSizeInBytes());
                return pageBuilder.build();
            }

            @Override
            public long getMemoryUsage() {
                // local state is almost zero
                return 0;
            }

            @Override
            public void deleteRows(Block rowIds) {
                for (int i = 0; i < rowIds.getPositionCount(); i++) {
                    int len = rowIds.getSliceLength(i);
                    Slice slice = rowIds.getSlice(i, 0, len);
                    String uri = wrikeEntityType.getBaseEndpoint() + "/" + slice.toStringUtf8();
                    Http.sync(uri, HttpRequest.Builder::DELETE);
                }
            }

            @Override
            public void updateRows(Page page, List<Integer> columnValueAndRowIdChannels) {
                for (int position = 0; position < page.getPositionCount(); position++) {
                    Block idBlock = page.getBlock(Iterables.getLast(columnValueAndRowIdChannels));
                    Slice idSlice = idBlock.getSlice(position, 0, idBlock.getSliceLength(position));
                    String uri = wrikeEntityType.getBaseEndpoint() + "/" + idSlice.toStringUtf8();

                    StringBuilder body = new StringBuilder();
                    for (int channel = 0; channel < columnValueAndRowIdChannels.size() - 1; channel++) {
                        Block block = page.getBlock(columnValueAndRowIdChannels.get(channel));
                        WrikeColumnHandle updatedColumn = wrikeTableHandle.updatedColumns().get(channel);
                        WrikeRestColumn restColumn = wrikeEntityType.getColumn(updatedColumn.name());
                        restColumn.toForm(block, position)
                                .ifPresent(formPair -> body
                                        .append('&')
                                        .append(formPair.parameter())
                                        .append('=')
                                        .append(formPair.encodedValue()));
                    }
                    Http.sync(uri, request -> request
                            .PUT(BodyPublishers.ofString(body.toString()))
                            .header("Content-Type", "application/x-www-form-urlencoded"));
                }
            }

            @Override
            public void close() {
                closed = true;
            }
        };
    }
}
