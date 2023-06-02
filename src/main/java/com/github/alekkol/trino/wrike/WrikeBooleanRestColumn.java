package com.github.alekkol.trino.wrike;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.BooleanType;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.trino.spi.type.BooleanType.BOOLEAN;

public class WrikeBooleanRestColumn implements WrikeRestColumn {
    private static final BooleanType type = BOOLEAN;

    private final String name;
    private final ColumnMetadata metadata;

    private WrikeBooleanRestColumn(String name) {
        this.name = Objects.requireNonNull(name);
        this.metadata = ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setNullable(true)
                .build();
    }

    public static WrikeBooleanRestColumn bool(String name) {
        return new WrikeBooleanRestColumn(name);
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
        } else if (raw instanceof Boolean bool) {
            type.writeBoolean(blockBuilder, bool);
        } else {
            throw new IllegalStateException("Not a boolean " + raw);
        }
    }

    @Override
    public Optional<FormField> toForm(Block block, int position) {
        Object raw = type.getObjectValue(null, block, position);
        if (raw == null) {
            return Optional.empty();
        } else if (raw instanceof Boolean bool) {
            return Optional.of(new FormField(name, bool.toString()));
        } else {
            throw new IllegalStateException("Not a boolean " + raw);
        }
    }
}
