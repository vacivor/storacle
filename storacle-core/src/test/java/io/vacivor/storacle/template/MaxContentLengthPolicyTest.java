package io.vacivor.storacle.template;

import io.vacivor.storacle.ObjectMetadata;
import io.vacivor.storacle.StoragePolicyViolationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MaxContentLengthPolicyTest {
    @Test
    void allowsContentWithinLimit() {
        MaxContentLengthPolicy policy = new MaxContentLengthPolicy(10);

        assertDoesNotThrow(() -> policy.validate(context(10L)));
    }

    @Test
    void allowsUnknownContentLength() {
        MaxContentLengthPolicy policy = new MaxContentLengthPolicy(10);

        assertDoesNotThrow(() -> policy.validate(context(null)));
    }

    @Test
    void rejectsContentAboveLimit() {
        MaxContentLengthPolicy policy = new MaxContentLengthPolicy(10);

        StoragePolicyViolationException exception = assertThrows(StoragePolicyViolationException.class,
                () -> policy.validate(context(11L)));

        assertEquals("CONTENT_LENGTH_EXCEEDED", exception.errorCode());
        assertEquals("Content length exceeds limit: 11 > 10", exception.getMessage());
    }

    @Test
    void rejectsNegativeLimit() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new MaxContentLengthPolicy(-1));

        assertEquals("maxContentLength must be >= 0", exception.getMessage());
    }

    private static UploadContext context(Long contentLength) {
        return new UploadContext("scene", "bucket", "key", "file.txt", "text/plain", contentLength, ObjectMetadata.empty());
    }
}
