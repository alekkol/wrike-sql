package com.github.alekkol.trino.wrike;

import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;

import java.net.URI;
import java.net.http.HttpRequest.BodyPublishers;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class WrikePageSink implements ConnectorPageSink {
    private final WrikeEntityType entityType;
    private CompletableFuture<String> future;

    public WrikePageSink(WrikeEntityType entityType) {
        this.entityType = Objects.requireNonNull(entityType);
    }

    @Override
    public CompletableFuture<?> appendPage(Page page) {
        StringBuilder body = new StringBuilder();
        for (int position = 0; position < page.getPositionCount(); position++) {
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Block block = page.getBlock(channel);
                WrikeRestColumn restColumn = entityType.getColumns().get(channel);
                restColumn.toForm(block, position).ifPresent(formPair -> body.append('&').append(formPair.encode()));
            }
        }
        URI uri = URI.create("https://www.wrike.com/api/v4" + entityType.getInsertEndpoint());
        return future = Http.async(request -> request.POST(BodyPublishers.ofString(body.toString())).uri(uri).header("Content-Type", "application/x-www-form-urlencoded"));
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish() {
        return future.thenCompose(response -> CompletableFuture.completedFuture(List.of()));
    }

    @Override
    public void abort() {
        if (!future.isDone()) {
            future.cancel(true);
        }
    }
}
