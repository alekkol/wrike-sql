package com.github.alekkol.trino.wrike;

import io.trino.spi.connector.ConnectorTransactionHandle;

public class WrikeConnectorTransactionHandle implements ConnectorTransactionHandle {
    public static final WrikeConnectorTransactionHandle INSTANCE = new WrikeConnectorTransactionHandle();
}
