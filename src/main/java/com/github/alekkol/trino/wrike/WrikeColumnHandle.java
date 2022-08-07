package com.github.alekkol.trino.wrike;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

public record WrikeColumnHandle(String name, Type type) implements ColumnHandle {
    @JsonCreator
    public WrikeColumnHandle(@JsonProperty("name") String name, @JsonProperty("type") Type type) {
        this.name = requireNonNull(name);
        this.type = requireNonNull(type);
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
}
