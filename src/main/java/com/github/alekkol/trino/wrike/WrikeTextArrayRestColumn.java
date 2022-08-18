package com.github.alekkol.trino.wrike;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;

import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.net.http.HttpRequest.BodyPublishers.noBody;
import static java.net.http.HttpRequest.BodyPublishers.ofString;
import static java.nio.charset.StandardCharsets.UTF_8;

public class WrikeTextArrayRestColumn implements WrikeRestColumn {
    private static final ArrayType type = new ArrayType(VARCHAR);
    private final String name;
    private final ColumnMetadata metadata;

    public WrikeTextArrayRestColumn(String name) {
        this.name = Objects.requireNonNull(name);
        this.metadata = ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setNullable(true)
                .build();
    }

    @Override
    public ColumnMetadata metadata() {
        return metadata;
    }

    @Override
    public HttpRequest.BodyPublisher write(Block block, int position) {
        Object raw = type.getObjectValue(null, block, position);
        if (raw == null) {
            return noBody();
        } else if (raw instanceof Collection<?> collection) {
            String paramValue = collection.stream()
                    .map(Object::toString)
                    .map(value -> "\"" + "\"")
                    .collect(Collectors.joining(",", "[", "]"));
            return ofString(URLEncoder.encode(paramValue, UTF_8));
        } else {
            throw new IllegalStateException("Not a collection: " + raw);
        }
    }

    @Override
    public Object readObject(Map<String, ?> json) {
        Object raw = json.get(name);
        if (raw instanceof Collection<?> collection) {
            Type elementType = type.getElementType();
            BlockBuilder blockBuilder = elementType.createBlockBuilder(null, collection.size());
            collection.forEach(element -> writeNativeValue(elementType, blockBuilder, element));
            return blockBuilder.build();
        } else {
            throw new IllegalStateException("Not a collection: " + raw);
        }
    }

    @Override
    public boolean isNull(Map<String, ?> json) {
        return json.get(name) == null;
    }
}
