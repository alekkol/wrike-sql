package com.github.alekkol.trino.wrike;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class WrikeTextArrayRestColumn implements WrikeRestColumn {
    private static final ArrayType type = new ArrayType(VARCHAR);
    private final String name;
    private final ColumnMetadata metadata;

    private WrikeTextArrayRestColumn(String name) {
        this.name = Objects.requireNonNull(name);
        this.metadata = ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setNullable(true)
                .build();
    }

    public static WrikeTextArrayRestColumn textArray(String name) {
        return new WrikeTextArrayRestColumn(name);
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
        } else if (raw instanceof Collection<?> collection) {
            Type elementType = type.getElementType();
            BlockBuilder arrayBlockBuilder = elementType.createBlockBuilder(null, collection.size());
            collection.forEach(element -> writeNativeValue(type.getElementType(), arrayBlockBuilder, element));
            Block arrayBlock = arrayBlockBuilder.build();
            type.writeObject(blockBuilder, arrayBlock);
        } else {
            throw new IllegalStateException("Not a collection: " + raw);
        }
    }

    @Override
    public Optional<FormField> toForm(Block block, int position) {
        Object raw = type.getObjectValue(null, block, position);
        if (raw == null) {
            return Optional.empty();
        } else if (raw instanceof Collection<?> collection) {
            String paramValue = collection.stream()
                    .map(Object::toString)
                    .map(value -> "\"" + "\"")
                    .collect(Collectors.joining(",", "[", "]"));
            return Optional.of(new FormField(name, paramValue));
        } else {
            throw new IllegalStateException("Not a collection: " + raw);
        }
    }
}
