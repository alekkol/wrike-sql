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

import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class WrikeNestedTextArrayRestColumn implements WrikeRestColumn {
    private static final ArrayType type = new ArrayType(VARCHAR);

    private final String ownColumn;
    private final String foreignColumn;
    private final ColumnMetadata metadata;

    private WrikeNestedTextArrayRestColumn(String name, String ownColumn, String foreignColumn) {
        this.ownColumn = Objects.requireNonNull(ownColumn);
        this.foreignColumn = Objects.requireNonNull(foreignColumn);
        this.metadata = ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setNullable(true)
                .build();
    }

    public static WrikeNestedTextArrayRestColumn nestedTextArray(String name, String ownColumn, String foreignColumn) {
        return new WrikeNestedTextArrayRestColumn(name, ownColumn, foreignColumn);
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
    public void toBlock(Map<String, ?> json, BlockBuilder blockBuilder) {
        Object raw = json.get(ownColumn);
        if (raw == null) {
            blockBuilder.appendNull();
        } else {
            if (raw instanceof Collection<?> collection) {
                Type elementType = type.getElementType();
                BlockBuilder arrayBlockBuilder = elementType.createBlockBuilder(null, collection.size());
                for (Object element : collection) {
                    final Object valueToAppend;
                    if (element instanceof Map<?, ?> nested) {
                        valueToAppend = nested.get(foreignColumn);
                    } else {
                        valueToAppend = element;
                    }
                    writeNativeValue(type.getElementType(), arrayBlockBuilder, valueToAppend);
                }
                Block arrayBlock = arrayBlockBuilder.build();
                type.writeObject(blockBuilder, arrayBlock);
            } else {
                throw new IllegalStateException("Not a collection: " + raw);
            }
        }
    }

    @Override
    public Optional<FormField> toForm(Block block, int position) {
        throw new UnsupportedOperationException("TODO");
    }
}
