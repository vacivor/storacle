package io.vacivor.storacle.example;

import java.util.List;

public record ListObjectsResponse(
        List<ObjectSummaryResponse> objects,
        String nextContinuationToken,
        boolean truncated
) {
}
