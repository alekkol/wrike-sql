package com.github.alekkol.trino.wrike;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

public record WrikeColumnHandle(String name) implements ColumnHandle {
    @JsonCreator
    public WrikeColumnHandle(@JsonProperty("name") String name) {
        this.name = requireNonNull(name);
    }

    @Override
    @JsonProperty("name")
    public String name() {
        return name;
    }
}
