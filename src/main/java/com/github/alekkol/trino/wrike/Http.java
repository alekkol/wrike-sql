package com.github.alekkol.trino.wrike;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static java.net.http.HttpClient.Redirect.NORMAL;

public final class Http {
    public static final HttpClient httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(NORMAL)
            .build();

    public static CompletableFuture<String> async(Consumer<HttpRequest.Builder> request) {
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .header("Authorization", "Bearer " + System.getProperty("com.github.alekkol.trino.wrike.token"));
        request.accept(requestBuilder);
        return httpClient.sendAsync(requestBuilder.build(), HttpResponse.BodyHandlers.ofString())
                .thenCompose(response -> {
                    if (response.statusCode() / 100 != 2) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("REST query failed: status=" + response.statusCode() + ", body=" + response.body()));
                    } else {
                        return CompletableFuture.completedFuture(response.body());
                    }
                });
    }

    public static String sync(Consumer<HttpRequest.Builder> request) {
        return async(request).join();
    }
}
