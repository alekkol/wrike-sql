package com.github.alekkol.trino.wrike;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;

import java.net.http.HttpRequest.BodyPublishers;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WrikePageSink implements ConnectorPageSink {
    private static final Pattern URI_PLACEHOLDER = Pattern.compile("\\$\\{(\\w+)}");

    private final WrikeEntityType entityType;
    private final Set<String> updatedIds = ConcurrentHashMap.newKeySet();

    public WrikePageSink(WrikeEntityType entityType) {
        this.entityType = Objects.requireNonNull(entityType);
    }

    @Override
    public CompletableFuture<?> appendPage(Page page) {
        List<WrikeRestColumn> columns = List.copyOf(entityType.getColumns());
        var columnToFormField = new HashMap<String, String>();
        for (int position = 0; position < page.getPositionCount(); position++) {
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Block block = page.getBlock(channel);
                WrikeRestColumn restColumn = columns.get(channel);
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

        String body = Joiner.on("&").withKeyValueSeparator('=').join(columnToFormField);
        return Http.async(insertEndpoint, request -> request
                        .POST(BodyPublishers.ofString(body))
                        .header("Content-Type", "application/x-www-form-urlencoded"))
                .thenAccept(response -> {
                    JsonNode responseJson;
                    try {
                        responseJson = new ObjectMapper().readTree(response);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    String id = responseJson.get(entityType.getPkColumn().metadata().getName())
                            .asText();
                    updatedIds.add(id);
                });
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish() {
        return CompletableFuture.completedFuture(updatedIds.stream()
                .map(Slices::utf8Slice)
                .toList());
    }

    @Override
    public void abort() {
    }
}
