package com.github.alekkol.trino.wrike;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;

import static java.util.Objects.requireNonNull;

public record WrikeColumnHandle(String name, boolean primaryKey) implements ColumnHandle {
    // https://trino.io/docs/current/develop/delete-and-update.html#the-rowid-column-abstraction
    private static final String ROW_ID_NAME = "rowId";

    @JsonCreator
    public WrikeColumnHandle(@JsonProperty("name") String name, @JsonProperty("primaryKey") boolean primaryKey) {
        this.name = requireNonNull(name);
        this.primaryKey = primaryKey;
    }

    public static WrikeColumnHandle rowId() {
        return new WrikeColumnHandle(ROW_ID_NAME, false);
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

    public boolean isRowId() {
        return ROW_ID_NAME.equals(name);
    }
}
