package com.github.alekkol.trino.wrike;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static io.trino.spi.type.TypeUtils.writeNativeValue;
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

        List<WrikeColumnHandle> wrikeColumnHandles = columns.stream()
                .map(WrikeColumnHandle.class::cast)
                .toList();
        List<Type> types = wrikeColumnHandles.stream()
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
                                response = httpClient.send(HttpRequest.newBuilder()
                                                .GET()
                                                .uri(URI.create("https://www.wrike.com/api/v4" + wrikeTableHandle.entityType().getEndpoint()))
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
                                JsonParser parser = objectMapper.createParser(response.body());
                                LowerCaseKeyJsonParser lowerCaseKeyJsonParser = new LowerCaseKeyJsonParser(parser);
                                //noinspection unchecked
                                result = objectMapper.readValue(lowerCaseKeyJsonParser, Map.class);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }

                            data = (List<Map<String, Object>>) requireNonNull(result.get("data"), "No 'data' in response");
                        }

                        return ++row < data.size();
                    }

                    private <T> T readValue(int field, Class<T> clazz) {
                        Map<String, Object> object = data.get(row);
                        if (object == null) {
                            throw new IllegalStateException("No value at index: " + row);
                        }
                        WrikeColumnHandle column = wrikeColumnHandles.get(field);
                        if (column == null) {
                            throw new IllegalStateException("No column at index: " + field);
                        }
                        return clazz.cast(object.get(column.name())); // 1:1 mapping REST field and DB column
                    }

                    @Override
                    public boolean getBoolean(int field) {
                        return readValue(field, Boolean.class);
                    }

                    @Override
                    public long getLong(int field) {
                        return readValue(field, Long.class);
                    }

                    @Override
                    public double getDouble(int field) {
                        return readValue(field, Double.class);
                    }

                    @Override
                    public Slice getSlice(int field) {
                        String value = readValue(field, String.class);
                        return Slices.utf8Slice(value);
                    }

                    @Override
                    public Object getObject(int field) {
                        Object value = readValue(field, Object.class);
                        if (value instanceof Collection<?> collection) {
                            Type elementType = ((ArrayType) getType(field)).getElementType();
                            BlockBuilder blockBuilder = elementType.createBlockBuilder(null, collection.size());
                            collection.forEach(element -> writeNativeValue(elementType, blockBuilder, element));
                            return blockBuilder.build();
                        } else {
                            return value;
                        }
                    }

                    @Override
                    public boolean isNull(int field) {
                        return readValue(field, Object.class) == null;
                    }

                    @Override
                    public void close() {
                    }
                };
            }
        };
    }
}
