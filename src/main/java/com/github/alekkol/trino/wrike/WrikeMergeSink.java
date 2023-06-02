package com.github.alekkol.trino.wrike;

import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.type.TinyintType;

import java.net.http.HttpRequest;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class WrikeMergeSink implements ConnectorMergeSink {
    private final WrikeEntityType entityType;

    public WrikeMergeSink(WrikeEntityType entityType) {
        this.entityType = Objects.requireNonNull(entityType);
    }

    @Override
    public void storeMergedRows(Page page) {
        for (int position = 0; position < page.getPositionCount(); position++) {
            Block opBLock = page.getBlock(page.getChannelCount() - 2);
            Block rowIdBlock = page.getBlock(page.getChannelCount() - 1);
            byte opByte = TinyintType.TINYINT.getByte(opBLock, position);
            if (opByte == DELETE_OPERATION_NUMBER) {
                entityType.getPkColumn().toForm(rowIdBlock, position)
                        .map(WrikeRestColumn.FormField::encodedValue)
                        .ifPresent(idToDelete -> {
                            String uri = entityType.getBaseEndpoint() + "/" + idToDelete;
                            Http.sync(uri, HttpRequest.Builder::DELETE);
                        });
            } else if (opByte == UPDATE_OPERATION_NUMBER) {
                Slice idSlice = rowIdBlock.getSlice(position, 0, rowIdBlock.getSliceLength(position));
                String uri = entityType.getBaseEndpoint() + "/" + idSlice.toStringUtf8();

                StringBuilder body = new StringBuilder();
                List<WrikeRestColumn> columns = entityType.getColumns();
                for (int channel = 0; channel < page.getChannelCount() - 2; channel++) {
                    Block block = page.getBlock(channel);
                    WrikeRestColumn restColumn = columns.get(channel);
                    // in Trino 418 there is no way to restrict batch with updated columns only
                    // see https://github.com/trinodb/trino/issues/15585
                    if (restColumn.isWritable()) {
                        restColumn.toForm(block, position)
                                .ifPresent(formPair -> body
                                        .append('&')
                                        .append(formPair.parameter())
                                        .append('=')
                                        .append(formPair.encodedValue()));
                    }
                }
                Http.sync(uri, request -> request
                        .PUT(HttpRequest.BodyPublishers.ofString(body.toString()))
                        .header("Content-Type", "application/x-www-form-urlencoded"));
            } else {
                throw new UnsupportedOperationException("Unsupported merge operation: " + opByte);
            }
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish() {
        return CompletableFuture.completedFuture(List.of());
    }
}
