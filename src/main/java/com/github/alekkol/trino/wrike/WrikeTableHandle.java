package com.github.alekkol.trino.wrike;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record WrikeTableHandle(WrikeEntityType entityType,
                               List<WrikeColumnHandle> updatedColumns,
                               Optional<String> id) implements ConnectorTableHandle {
    @JsonCreator
    public WrikeTableHandle(@JsonProperty("entityType") WrikeEntityType entityType,
                            @JsonProperty("updatedColumns") List<WrikeColumnHandle> updatedColumns,
                            @JsonProperty("id") Optional<String> id) {
        this.entityType = requireNonNull(entityType);
        this.updatedColumns = requireNonNull(updatedColumns);
        this.id = requireNonNull(id);
    }

    public WrikeTableHandle(WrikeEntityType entityType, Optional<String> id) {
        this(entityType, List.of(), id);
    }

    public WrikeTableHandle withUpdatedColumns(List<WrikeColumnHandle> updatedColumns) {
        return new WrikeTableHandle(entityType, updatedColumns, id);
    }

    @Override
    @JsonProperty("entityType")
    public WrikeEntityType entityType() {
        return entityType;
    }

    @Override
    @JsonProperty("updatedColumns")
    public List<WrikeColumnHandle> updatedColumns() {
        return updatedColumns;
    }

    @Override
    @JsonProperty("id")
    public Optional<String> id() {
        return id;
    }
}
