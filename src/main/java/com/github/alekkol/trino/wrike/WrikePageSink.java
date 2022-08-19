package com.github.alekkol.trino.wrike;

import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class WrikePageSink implements ConnectorPageSink {
    private final WrikeEntityType entityType;
    private CompletableFuture<?> future;

    public WrikePageSink(WrikeEntityType entityType) {
        this.entityType = Objects.requireNonNull(entityType);
    }

    @Override
    public CompletableFuture<?> appendPage(Page page) {
        ArrayList<BodyPublisher> bodyPublishers = new ArrayList<>();
        for (int position = 0; position < page.getPositionCount(); position++) {
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Block block = page.getBlock(channel);
                BodyPublisher bodyPublisher = entityType.getColumns().get(channel).write(block, position);
                bodyPublishers.add(bodyPublisher);
            }
        }
        BodyPublisher body = HttpRequest.BodyPublishers.concat(bodyPublishers.toArray(new BodyPublisher[0]));
        HttpRequest request = HttpRequest.newBuilder()
                .POST(body)
                .uri(URI.create("https://www.wrike.com/api/v4" + entityType.getBaseEndpoint()))
                .header("Authorization", "Bearer " + System.getProperty("com.github.alekkol.trino.wrike.token"))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .build();
        future = HttpClient.newHttpClient().sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenCompose(response -> {
                    if (response.statusCode() / 100 != 2) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("REST query failed: status=" + response.statusCode() + ", body=" + response.body()));
                    } else {
                        return CompletableFuture.completedFuture(response.body());
                    }
                });
        return future;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish() {
        return CompletableFuture.completedFuture(List.of());
    }

    @Override
    public void abort() {
        if (!future.isDone()) {
            future.cancel(true);
        }
    }
}
