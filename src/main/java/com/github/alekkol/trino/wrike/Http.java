package com.github.alekkol.trino.wrike;

import io.airlift.log.Logger;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static java.net.http.HttpClient.Redirect.NORMAL;
import static java.util.Objects.requireNonNull;

public final class Http {
    private static final Logger LOG = Logger.get(Http.class);
    private static final String BASE_URL;
    private static final String TOKEN;

    static {
        BASE_URL = System.getProperty("com.github.alekkol.trino.wrike.url", "https://www.wrike.com/api/v4");
        TOKEN = requireNonNull(System.getProperty("com.github.alekkol.trino.wrike.token"), "empty token");
    }

    public static final HttpClient httpClient = HttpClient.newBuilder()
            .version(Version.HTTP_2)
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(NORMAL)
            .build();

    public static CompletableFuture<String> async(CharSequence path, Consumer<HttpRequest.Builder> builder) {
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .header("Authorization", "Bearer " + TOKEN);
        builder.accept(requestBuilder);
        Instant start = Instant.now();
        requestBuilder.uri(URI.create(BASE_URL + path));
        HttpRequest request = requestBuilder.build();
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .handle((response, throwable) -> {
                    LOG.info("Sent request [%s] %s, got %s, elapsed %d ms",
                            request.method(),
                            request.uri(),
                            throwable != null ? throwable.getMessage() : response.statusCode(),
                            Duration.between(start, Instant.now()).toMillis());
                    return response;
                })
                .thenCompose(response -> {
                    if (response.statusCode() / 100 != 2) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("status=" + response.statusCode() + ", body=" + response.body()));
                    } else {
                        return CompletableFuture.completedFuture(response.body());
                    }
                });
    }

    public static String sync(CharSequence path, Consumer<HttpRequest.Builder> builder) {
        try {
            return async(path, builder).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException(Objects.toString(e.getMessage(), "Query failed"), e.getCause());
        }
    }
}
