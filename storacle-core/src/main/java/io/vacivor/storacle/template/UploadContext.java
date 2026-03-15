package io.vacivor.storacle.template;

import io.vacivor.storacle.ObjectMetadata;

public record UploadContext(
        String scene,
        String bucket,
        String objectKey,
        String originalFilename,
        String contentType,
        Long contentLength,
        ObjectMetadata metadata
) {
}
