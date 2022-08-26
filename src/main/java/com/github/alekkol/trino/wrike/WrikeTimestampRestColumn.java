package com.github.alekkol.trino.wrike;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.TimestampType;

import java.net.http.HttpRequest;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static java.net.http.HttpRequest.BodyPublishers.noBody;
import static java.net.http.HttpRequest.BodyPublishers.ofString;

public class WrikeTimestampRestColumn implements WrikeRestColumn {
    private static final TimestampType type = TIMESTAMP_SECONDS;

    private final String name;
    private final ColumnMetadata metadata;

    private WrikeTimestampRestColumn(String name) {
        this.name = Objects.requireNonNull(name);
        this.metadata = ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setNullable(true)
                .build();
    }

    public static WrikeTimestampRestColumn timestamp(String name) {
        return new WrikeTimestampRestColumn(name);
    }

    @Override
    public boolean isPrimaryKey() {
        return false;
    }

    @Override
    public ColumnMetadata metadata() {
        return metadata;
    }

    @Override
    public void read(Map<String, ?> json, BlockBuilder blockBuilder) {
        Object raw = json.get(name);
        if (raw == null) {
            blockBuilder.appendNull();
        } else if (raw instanceof String date) {
            long epochMicros = Instant.parse(date).getEpochSecond() * 1_000_000;
            blockBuilder.writeLong(epochMicros).closeEntry();
        } else {
            throw new IllegalStateException("Not a string " + raw);
        }
    }

    @Override
    public HttpRequest.BodyPublisher write(Block block, int position) {
        Object raw = type.getObjectValue(null, block, position);
        if (raw == null) {
            return noBody();
        } else if (raw instanceof String text) {
            return ofString(name + "=" + text);
        } else {
            throw new IllegalStateException("Not a string " + raw);
        }
    }
}
