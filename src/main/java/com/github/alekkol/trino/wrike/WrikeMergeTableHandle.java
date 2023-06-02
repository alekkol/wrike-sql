package com.github.alekkol.trino.wrike;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorTableHandle;

public record WrikeMergeTableHandle(@JsonProperty("tableHandle") WrikeTableHandle tableHandle)
        implements ConnectorMergeTableHandle {
    @Override
    public ConnectorTableHandle getTableHandle() {
        return tableHandle;
    }
}
