package io.vacivor.storacle.example;

import java.time.Instant;

public record ApiErrorResponse(
        Instant timestamp,
        int status,
        String error,
        String errorCode,
        String message,
        String path
) {
}
