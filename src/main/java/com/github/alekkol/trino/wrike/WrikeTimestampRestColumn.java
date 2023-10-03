package com.github.alekkol.trino.wrike;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.TimestampType;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;

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
    public boolean isWritable() {
        return false;
    }

    @Override
    public void toBlock(Map<String, ?> json, BlockBuilder blockBuilder) {
        Object raw = json.get(name);
        if (raw == null) {
            blockBuilder.appendNull();
        } else if (raw instanceof String date) {
            long epochMicros = Instant.parse(date).getEpochSecond() * 1_000_000;
            BIGINT.writeLong(blockBuilder, epochMicros);
        } else {
            throw new IllegalStateException("Not a string " + raw);
        }
    }

    @Override
    public Optional<FormField> toForm(Block block, int position) {
        Object raw = type.getObjectValue(null, block, position);
        if (raw == null) {
            return Optional.empty();
        } else if (raw instanceof String text) {
            return Optional.of(new FormField(name, text));
        } else {
            throw new IllegalStateException("Not a string " + raw);
        }
    }
}
