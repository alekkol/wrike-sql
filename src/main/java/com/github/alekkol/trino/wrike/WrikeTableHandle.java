package com.github.alekkol.trino.wrike;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.Objects;
import java.util.Optional;

public record WrikeTableHandle(WrikeEntityType entityType, Optional<String> id) implements ConnectorTableHandle {
    @JsonCreator
    public WrikeTableHandle(@JsonProperty("entityType") WrikeEntityType entityType,
                            @JsonProperty("id") Optional<String> id) {
        this.entityType = Objects.requireNonNull(entityType);
        this.id = Objects.requireNonNull(id);
    }

    @Override
    @JsonProperty("entityType")
    public WrikeEntityType entityType() {
        return entityType;
    }

    @Override
    @JsonProperty("id")
    public Optional<String> id() {
        return id;
    }
}
