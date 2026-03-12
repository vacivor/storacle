package io.vacivor.storacle.example;

import java.time.Instant;

public record ObjectSummaryResponse(
        String bucket,
        String key,
        long size,
        Instant lastModified,
        String eTag
) {
}
