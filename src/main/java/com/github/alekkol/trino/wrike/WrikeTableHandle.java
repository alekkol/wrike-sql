package com.github.alekkol.trino.wrike;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record WrikeTableHandle(WrikeEntityType entityType,
                               Optional<List<String>> ids) implements ConnectorTableHandle {
    @JsonCreator
    public WrikeTableHandle(@JsonProperty("entityType") WrikeEntityType entityType,
                            @JsonProperty("ids") Optional<List<String>> ids) {
        this.entityType = requireNonNull(entityType);
        this.ids = requireNonNull(ids);
    }

    @Override
    @JsonProperty("entityType")
    public WrikeEntityType entityType() {
        return entityType;
    }

    @Override
    @JsonProperty("ids")
    public Optional<List<String>> ids() {
        return ids;
    }
}
