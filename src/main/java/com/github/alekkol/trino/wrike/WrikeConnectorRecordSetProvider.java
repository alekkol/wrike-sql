package com.github.alekkol.trino.wrike;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static java.net.http.HttpClient.Redirect.NORMAL;
import static java.util.Objects.requireNonNull;

public class WrikeConnectorRecordSetProvider implements ConnectorRecordSetProvider {
    private final HttpClient httpClient;

    public WrikeConnectorRecordSetProvider() {
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .connectTimeout(Duration.ofSeconds(10))
                .followRedirects(NORMAL)
                .build();
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction,
                                  ConnectorSession session,
                                  ConnectorSplit split,
                                  ConnectorTableHandle table,
                                  List<? extends ColumnHandle> columns) {
        WrikeTableHandle wrikeTableHandle = (WrikeTableHandle) table;
        WrikeEntityType wrikeEntityType = wrikeTableHandle.entityType();
        List<WrikeRestColumn> wrikeRestColumns = columns.stream()
                .map(WrikeColumnHandle.class::cast)
                .map(WrikeColumnHandle::name)
                .map(wrikeEntityType::getColumn)
                .toList();
        List<Type> types = wrikeRestColumns.stream()
                .map(WrikeRestColumn::metadata)
                .map(ColumnMetadata::getType)
                .toList();

        return new RecordSet() {
            @Override
            public List<Type> getColumnTypes() {
                return types;
            }

            @Override
            public RecordCursor cursor() {
                return new RecordCursor() {
                    final long startNanoTime = System.nanoTime();
                    final AtomicLong completedBytes = new AtomicLong();
                    int row = -1;
                    List<Map<String, Object>> data;

                    @Override
                    public long getCompletedBytes() {
                        return completedBytes.get();
                    }

                    @Override
                    public long getReadTimeNanos() {
                        return System.nanoTime() - startNanoTime;
                    }

                    @Override
                    public Type getType(int field) {
                        return types.get(field);
                    }

                    @Override
                    public boolean advanceNextPosition() {
                        if (data == null) {
                            HttpResponse<String> response;
                            try {
                                final StringBuilder uriBuilder = new StringBuilder();
                                uriBuilder.append("https://www.wrike.com/api/v4");
                                wrikeTableHandle.id().ifPresentOrElse(
                                        id -> uriBuilder.append(wrikeEntityType.getBaseEndpoint()).append("/").append(id),
                                        () -> uriBuilder.append(wrikeEntityType.getSelectAllEndpoint()));
                                response = httpClient.send(
                                        HttpRequest.newBuilder()
                                                .GET()
                                                .uri(URI.create(uriBuilder.toString()))
                                                .header("Authorization", "Bearer " + System.getProperty("com.github.alekkol.trino.wrike.token"))
                                                .build(),
                                        HttpResponse.BodyHandlers.ofString());
                            } catch (IOException | InterruptedException e) {
                                if (e instanceof InterruptedException) {
                                    Thread.currentThread().interrupt();
                                }
                                throw new RuntimeException(e);
                            }
                            if (response.statusCode() / 100 != 2) {
                                throw new IllegalStateException("REST query failed: status=" + response.statusCode() + ", body=" + response.body());
                            }
                            ObjectMapper objectMapper = new ObjectMapper();
                            Map<String, Object> result;
                            try {
                                //noinspection unchecked
                                result = objectMapper.readValue(response.body(), Map.class);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }

                            data = (List<Map<String, Object>>) requireNonNull(result.get("data"), "No 'data' in response");
                        }

                        return ++row < data.size();
                    }

                    private Map<String, ?> currentRow() {
                        return data.get(row);
                    }

                    @Override
                    public boolean getBoolean(int field) {
                        return wrikeRestColumns.get(field).readBoolean(currentRow());
                    }

                    @Override
                    public long getLong(int field) {
                        return wrikeRestColumns.get(field).readLong(currentRow());
                    }

                    @Override
                    public double getDouble(int field) {
                        return wrikeRestColumns.get(field).readDouble(currentRow());
                    }

                    @Override
                    public Slice getSlice(int field) {
                        return wrikeRestColumns.get(field).readSlice(currentRow());
                    }

                    @Override
                    public Object getObject(int field) {
                        return wrikeRestColumns.get(field).readObject(currentRow());
                    }

                    @Override
                    public boolean isNull(int field) {
                        return wrikeRestColumns.get(field).isNull(currentRow());
                    }

                    @Override
                    public void close() {
                    }
                };
            }
        };
    }
}
