package com.github.alekkol.trino.wrike;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnMetadata;

import java.net.http.HttpRequest;
import java.util.Map;

public interface WrikeRestColumn {
    boolean isPrimaryKey();

    ColumnMetadata metadata();

    void read(Map<String, ?> json, BlockBuilder blockBuilder);

    HttpRequest.BodyPublisher write(Block block, int position);
}
