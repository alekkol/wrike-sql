package com.github.alekkol.trino.wrike;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

public record WrikeColumnHandle(String name, Type type, boolean nullable) implements ColumnHandle {
    @JsonCreator
    public WrikeColumnHandle(@JsonProperty("name") String name, @JsonProperty("type") Type type, @JsonProperty("nullable") boolean nullable) {
        this.name = requireNonNull(name);
        this.type = requireNonNull(type);
        this.nullable = nullable;
    }

    public WrikeColumnHandle(ColumnMetadata metadata) {
        this(metadata.getName(), metadata.getType(), metadata.isNullable());
    }

    @Override
    @JsonProperty("name")
    public String name() {
        return name;
    }

    @Override
    @JsonProperty("type")
    public Type type() {
        return type;
    }

    @Override
    @JsonProperty("nullable")
    public boolean nullable() {
        return nullable;
    }

    public ColumnMetadata toColumnMetadata() {
        return ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setNullable(nullable)
                .build();
    }
}
