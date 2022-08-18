package com.github.alekkol.trino.wrike;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.VarcharType;

import java.net.http.HttpRequest;
import java.util.Map;
import java.util.Objects;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.net.http.HttpRequest.BodyPublishers.noBody;
import static java.net.http.HttpRequest.BodyPublishers.ofString;

public class WrikeTextRestColumn implements WrikeRestColumn {
    private static final VarcharType type = VARCHAR;

    private final String name;
    private final ColumnMetadata metadata;

    public WrikeTextRestColumn(String name) {
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
        } else if (raw instanceof String text) {
            return ofString(name + "=" + text);
        } else {
            throw new IllegalStateException("Not a string " + raw);
        }
    }

    @Override
    public Slice readSlice(Map<String, ?> json) {
        Object raw = json.get(name);
        if (raw instanceof String text) {
            return Slices.utf8Slice(text);
        } else {
            throw new IllegalStateException("Not a string " + raw);
        }
    }

    @Override
    public boolean isNull(Map<String, ?> json) {
        return json.get(name) == null;
    }
}
