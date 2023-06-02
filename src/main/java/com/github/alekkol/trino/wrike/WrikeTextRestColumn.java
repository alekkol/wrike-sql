package com.github.alekkol.trino.wrike;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.VarcharType;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.trino.spi.type.VarcharType.VARCHAR;

public class WrikeTextRestColumn implements WrikeRestColumn {
    private static final VarcharType type = VARCHAR;

    private final String requestColumn;
    private final String responseColumn;
    private final boolean primaryKey;
    private final ColumnMetadata metadata;
    private boolean readOnly;

    private WrikeTextRestColumn(String requestColumn, String responseColumn, boolean primaryKey) {
        this.requestColumn = Objects.requireNonNull(requestColumn);
        this.responseColumn = Objects.requireNonNull(responseColumn);
        this.metadata = ColumnMetadata.builder()
                .setName(responseColumn)
                .setType(type)
                .setNullable(true)
                .build();
        this.primaryKey = primaryKey;
        this.readOnly = true;
    }

    public static WrikeTextRestColumn primaryKey(String name) {
        return new WrikeTextRestColumn(name, name, true);
    }

    public static WrikeTextRestColumn text(String name) {
        return new WrikeTextRestColumn(name, name, false);
    }

    public static WrikeTextRestColumn text(String requestColumn, String responseColumn) {
        return new WrikeTextRestColumn(requestColumn, responseColumn, false);
    }

    public WrikeTextRestColumn writable() {
        this.readOnly = false;
        return this;
    }

    @Override
    public boolean isPrimaryKey() {
        return primaryKey;
    }

    @Override
    public ColumnMetadata metadata() {
        return metadata;
    }

    @Override
    public boolean isWritable() {
        return !readOnly;
    }

    @Override
    public void toBlock(Map<String, ?> json, BlockBuilder blockBuilder) {
        Object raw = json.get(responseColumn);
        if (raw == null) {
            blockBuilder.appendNull();
        } else if (raw instanceof String text) {
            type.writeSlice(blockBuilder, Slices.utf8Slice(text));
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
            return Optional.of(new FormField(requestColumn, text));
        } else {
            throw new IllegalStateException("Not a string " + raw);
        }
    }
}
