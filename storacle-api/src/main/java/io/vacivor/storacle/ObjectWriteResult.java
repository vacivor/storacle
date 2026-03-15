package io.vacivor.storacle;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public record ObjectWriteResult(
        ObjectPath path,
        String originalFilename,
        String contentType,
        Long contentLength,
        String eTag,
        String versionId,
        Map<ChecksumAlgorithm, String> checksums
) {
    public ObjectWriteResult {
        Objects.requireNonNull(path, "path must not be null");
        checksums = checksums == null ? Map.of() : Map.copyOf(new LinkedHashMap<>(checksums));
    }

    public ObjectWriteResult(ObjectPath path, String eTag, String versionId) {
        this(path, null, null, null, eTag, versionId, Map.of());
    }

    public ObjectWriteResult(ObjectPath path, String eTag, String versionId, Map<ChecksumAlgorithm, String> checksums) {
        this(path, null, null, null, eTag, versionId, checksums);
    }

    public ObjectWriteResult(ObjectPath path, String originalFilename, String eTag, String versionId,
                             Map<ChecksumAlgorithm, String> checksums) {
        this(path, originalFilename, null, null, eTag, versionId, checksums);
    }

    public ObjectMetadata metadata() {
        return ChecksumMetadata.withChecksums(
                ObjectMetadata.builder()
                        .contentType(contentType)
                        .contentLength(contentLength)
                        .build(),
                checksums
        );
    }

    public String checksum(ChecksumAlgorithm algorithm) {
        String value = checksumOrNull(algorithm);
        if (value == null) {
            throw new IllegalArgumentException("Missing checksum for algorithm: " + algorithm);
        }
        return value;
    }

    public String checksumOrNull(ChecksumAlgorithm algorithm) {
        Objects.requireNonNull(algorithm, "algorithm must not be null");
        return checksums.get(algorithm);
    }
}
