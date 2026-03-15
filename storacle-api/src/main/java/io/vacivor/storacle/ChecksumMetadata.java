package io.vacivor.storacle;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public final class ChecksumMetadata {
    private static final String PREFIX = "checksum-";

    private ChecksumMetadata() {
    }

    public static String userMetadataKey(ChecksumAlgorithm algorithm) {
        Objects.requireNonNull(algorithm, "algorithm must not be null");
        return PREFIX + algorithm.value().toLowerCase(Locale.ROOT);
    }

    public static Map<ChecksumAlgorithm, String> extract(Map<String, String> userMetadata) {
        Objects.requireNonNull(userMetadata, "userMetadata must not be null");
        Map<ChecksumAlgorithm, String> checksums = new LinkedHashMap<>();
        for (ChecksumAlgorithm algorithm : ChecksumAlgorithm.values()) {
            String value = userMetadata.get(userMetadataKey(algorithm));
            if (value != null && !value.isBlank()) {
                checksums.put(algorithm, value);
            }
        }
        return Map.copyOf(checksums);
    }

    public static ObjectMetadata withChecksums(ObjectMetadata metadata, Map<ChecksumAlgorithm, String> checksums) {
        Objects.requireNonNull(metadata, "metadata must not be null");
        Objects.requireNonNull(checksums, "checksums must not be null");

        ObjectMetadata.Builder builder = ObjectMetadata.builder(metadata);
        Map<String, String> mergedUserMetadata = new LinkedHashMap<>(metadata.userMetadata());
        for (Map.Entry<ChecksumAlgorithm, String> entry : checksums.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null && !entry.getValue().isBlank()) {
                mergedUserMetadata.put(userMetadataKey(entry.getKey()), entry.getValue());
            }
        }
        builder.userMetadata(mergedUserMetadata);
        return builder.build();
    }
}
