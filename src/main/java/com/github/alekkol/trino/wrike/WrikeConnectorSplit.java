package com.github.alekkol.trino.wrike;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Objects;

@SuppressWarnings("ClassCanBeRecord") // because implements interface with default methods
public class WrikeConnectorSplit implements ConnectorSplit {
    private final List<HostAddress> addresses;

    @JsonCreator
    public WrikeConnectorSplit(@JsonProperty("addresses") List<HostAddress> addresses) {
        this.addresses = Objects.requireNonNull(addresses);
    }

    @Override
    public boolean isRemotelyAccessible() {
        return false;
    }

    @Override
    @JsonProperty("addresses")
    public List<HostAddress> getAddresses() {
        return addresses;
    }

    @Override
    public Object getInfo() {
        return this;
    }
}
