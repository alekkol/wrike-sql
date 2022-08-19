package com.github.alekkol.trino.wrike;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;

import static java.util.Objects.requireNonNull;

public record WrikeColumnHandle(String name, boolean primaryKey) implements ColumnHandle {
    @JsonCreator
    public WrikeColumnHandle(@JsonProperty("name") String name, @JsonProperty("primaryKey") boolean primaryKey) {
        this.name = requireNonNull(name);
        this.primaryKey = primaryKey;
    }

    @Override
    @JsonProperty("name")
    public String name() {
        return name;
    }

    @JsonProperty("primaryKey")
    public boolean primaryKey() {
        return primaryKey;
    }
}
