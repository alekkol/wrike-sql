package com.github.alekkol.trino.wrike;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

public class WrikeConnectionFactory implements ConnectorFactory {
    @Override
    public String getName() {
        return "wrike_rest";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        return new WrikeConnector(context.getNodeManager());
    }
}
