package com.github.alekkol.trino.wrike;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record WrikeTableHandle(WrikeEntityType entityType,
                               List<WrikeColumnHandle> updatedColumns,
                               Optional<List<String>> ids) implements ConnectorTableHandle {
    @JsonCreator
    public WrikeTableHandle(@JsonProperty("entityType") WrikeEntityType entityType,
                            @JsonProperty("updatedColumns") List<WrikeColumnHandle> updatedColumns,
                            @JsonProperty("ids") Optional<List<String>> ids) {
        this.entityType = requireNonNull(entityType);
        this.updatedColumns = requireNonNull(updatedColumns);
        this.ids = requireNonNull(ids);
    }

    public WrikeTableHandle(WrikeEntityType entityType, Optional<List<String>> ids) {
        this(entityType, List.of(), ids);
    }

    public WrikeTableHandle withUpdatedColumns(List<WrikeColumnHandle> updatedColumns) {
        return new WrikeTableHandle(entityType, updatedColumns, ids);
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
    @JsonProperty("ids")
    public Optional<List<String>> ids() {
        return ids;
    }
}
