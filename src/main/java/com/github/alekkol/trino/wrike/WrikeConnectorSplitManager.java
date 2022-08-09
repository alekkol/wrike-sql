package com.github.alekkol.trino.wrike;

import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

public class WrikeConnectorSplitManager implements ConnectorSplitManager {
    private final NodeManager nodeManager;

    public WrikeConnectorSplitManager(NodeManager nodeManager) {
        this.nodeManager = Objects.requireNonNull(nodeManager);
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle table, DynamicFilter dynamicFilter, Constraint constraint) {
        List<HostAddress> workerAddresses = nodeManager.getRequiredWorkerNodes().stream()
                .map(Node::getHostAndPort)
                .toList();
        return new FixedSplitSource(List.of(new WrikeConnectorSplit(workerAddresses)));
    }
}
