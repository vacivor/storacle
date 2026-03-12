package io.vacivor.storacle.example;

public record StorageObjectResponse(
        String bucket,
        String key,
        String eTag,
        String versionId
) {
}
