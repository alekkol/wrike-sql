package com.github.alekkol.trino.wrike;

import io.trino.spi.connector.ConnectorSplitManager;

public class WrikeConnectorSplitManager implements ConnectorSplitManager {
    public static final WrikeConnectorSplitManager INSTANCE = new WrikeConnectorSplitManager();
}
