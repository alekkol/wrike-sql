package com.github.alekkol.trino.wrike;

import com.google.common.base.Joiner;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;

import java.net.URI;
import java.net.http.HttpRequest.BodyPublishers;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WrikePageSink implements ConnectorPageSink {
    private static final Pattern URI_PLACEHOLDER = Pattern.compile("\\$\\{(\\w+)}");

    private final WrikeEntityType entityType;
    private CompletableFuture<String> future;

    public WrikePageSink(WrikeEntityType entityType) {
        this.entityType = Objects.requireNonNull(entityType);
    }

    @Override
    public CompletableFuture<?> appendPage(Page page) {
        var columnToFormField = new HashMap<String, String>();
        for (int position = 0; position < page.getPositionCount(); position++) {
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Block block = page.getBlock(channel);
                WrikeRestColumn restColumn = entityType.getColumns().get(channel);
                restColumn.toForm(block, position)
                        .ifPresent(formField -> columnToFormField.put(formField.parameter(), formField.encodedValue()));
            }
        }

        final StringBuilder insertEndpoint = new StringBuilder();
        final Matcher regexMatcher = URI_PLACEHOLDER.matcher(entityType.getInsertEndpoint());
        while (regexMatcher.find()) {
            final String placeholder = regexMatcher.group(1);
            final String value = columnToFormField.remove(placeholder);
            if (value != null) {
                //final String replaceSubstring = regexMatcher.group(1).replace(placeholder, value);
                regexMatcher.appendReplacement(insertEndpoint, value);
            }
        }
        regexMatcher.appendTail(insertEndpoint);
        URI uri = URI.create("https://www.wrike.com/api/v4" + insertEndpoint);

        String body = Joiner.on("&").withKeyValueSeparator('=').join(columnToFormField);
        return future = Http.async(request -> request
                .POST(BodyPublishers.ofString(body))
                .uri(uri)
                .header("Content-Type", "application/x-www-form-urlencoded"));
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
