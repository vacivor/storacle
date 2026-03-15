package io.vacivor.storacle.example;

import java.util.Map;

public record ObjectMetadataResponse(
        String bucket,
        String key,
        String contentType,
        Long contentLength,
        String contentDisposition,
        String cacheControl,
        Map<String, String> userMetadata,
        Map<String, String> checksums
) {
}
