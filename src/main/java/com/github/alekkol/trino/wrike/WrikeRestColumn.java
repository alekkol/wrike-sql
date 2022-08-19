package com.github.alekkol.trino.wrike;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnMetadata;

import java.net.http.HttpRequest;
import java.util.Map;

public interface WrikeRestColumn {
    boolean isPrimaryKey();

    ColumnMetadata metadata();

    HttpRequest.BodyPublisher write(Block block, int position);

    default boolean readBoolean(Map<String, ?> json) {
        throw new UnsupportedOperationException();
    }

    default long readLong(Map<String, ?> json) {
        throw new UnsupportedOperationException();
    }

    default double readDouble(Map<String, ?> json) {
        throw new UnsupportedOperationException();
    }

    default Slice readSlice(Map<String, ?> json) {
        throw new UnsupportedOperationException();
    }

    default Object readObject(Map<String, ?> json) {
        throw new UnsupportedOperationException();
    }

    default boolean isNull(Map<String, ?> json)  {
        throw new UnsupportedOperationException();
    }
}
