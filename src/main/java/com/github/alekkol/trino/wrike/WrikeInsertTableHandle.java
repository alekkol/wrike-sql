package com.github.alekkol.trino.wrike;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorInsertTableHandle;

public record WrikeInsertTableHandle(WrikeEntityType entityType) implements ConnectorInsertTableHandle {
    @JsonCreator
    public WrikeInsertTableHandle(@JsonProperty("entityType") WrikeEntityType entityType) {
        this.entityType = entityType;
    }

    @Override
    @JsonProperty("entityType")
    public WrikeEntityType entityType() {
        return entityType;
    }
}
