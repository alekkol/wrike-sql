package com.github.alekkol.trino.wrike;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnMetadata;

import java.net.URLEncoder;
import java.util.Map;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

public interface WrikeRestColumn {
    record FormField(String parameter, String value) {
        public String encodedValue() {
            return URLEncoder.encode(value, UTF_8);
        }
    }

    boolean isPrimaryKey();

    ColumnMetadata metadata();

    void toBlock(Map<String, ?> json, BlockBuilder blockBuilder);

    Optional<FormField> toForm(Block block, int position);
}
