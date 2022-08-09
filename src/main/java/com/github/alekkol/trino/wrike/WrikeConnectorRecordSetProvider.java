package com.github.alekkol.trino.wrike;

import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
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
import java.util.concurrent.atomic.AtomicLong;

public class WrikeConnectorRecordSetProvider implements ConnectorRecordSetProvider {
    private final HttpClient httpClient;

    public WrikeConnectorRecordSetProvider() {
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction,
                                  ConnectorSession session,
                                  ConnectorSplit split,
                                  ConnectorTableHandle table,
                                  List<? extends ColumnHandle> columns) {
        WrikeTableHandle wrikeTableHandle = (WrikeTableHandle) table;

        List<Type> types = columns.stream()
                .map(WrikeColumnHandle.class::cast)
                .map(WrikeColumnHandle::type)
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
                        try {
                            HttpResponse<String> response = httpClient.send(HttpRequest.newBuilder()
                                            .GET()
                                            .uri(URI.create("https://wrike.com/api/v4" + wrikeTableHandle.entityType().getEndpoint()))
                                            .header("Authorization", "Bearer " + System.getProperty("com.github.alekkol.trino.wrike.token"))
                                            .build(),
                                    HttpResponse.BodyHandlers.ofString());
                        } catch (IOException | InterruptedException e) {
                            if (e instanceof InterruptedException) {
                                Thread.currentThread().interrupt();
                            }
                            throw new RuntimeException(e);
                        }

                        return false;
                    }

                    @Override
                    public boolean getBoolean(int field) {
                        return false;
                    }

                    @Override
                    public long getLong(int field) {
                        return 0;
                    }

                    @Override
                    public double getDouble(int field) {
                        return 0;
                    }

                    @Override
                    public Slice getSlice(int field) {
                        return null;
                    }

                    @Override
                    public Object getObject(int field) {
                        return null;
                    }

                    @Override
                    public boolean isNull(int field) {
                        return false;
                    }

                    @Override
                    public void close() {
                    }
                };
            }
        };
    }
}
