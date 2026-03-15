package io.vacivor.storacle.template;

import io.vacivor.storacle.ObjectMetadata;
import io.vacivor.storacle.StoragePolicyViolationException;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AllowlistContentTypePolicyTest {
    @Test
    void allowsConfiguredContentType() {
        AllowlistContentTypePolicy policy = new AllowlistContentTypePolicy(Set.of("image/png", "image/jpeg"));

        assertDoesNotThrow(() -> policy.validate(context("image/png")));
    }

    @Test
    void normalizesConfiguredContentTypes() {
        AllowlistContentTypePolicy policy = new AllowlistContentTypePolicy(Set.of(" Image/PNG "));

        assertDoesNotThrow(() -> policy.validate(context("image/png")));
        assertEquals(Set.of("image/png"), policy.allowedContentTypes());
    }

    @Test
    void rejectsUnconfiguredContentType() {
        AllowlistContentTypePolicy policy = new AllowlistContentTypePolicy(Set.of("image/png"));

        StoragePolicyViolationException exception = assertThrows(StoragePolicyViolationException.class,
                () -> policy.validate(context("application/pdf")));

        assertEquals("CONTENT_TYPE_NOT_ALLOWED", exception.errorCode());
        assertEquals("Content type not allowed: application/pdf", exception.getMessage());
    }

    @Test
    void rejectsMissingContentType() {
        AllowlistContentTypePolicy policy = new AllowlistContentTypePolicy(Set.of("image/png"));

        StoragePolicyViolationException exception = assertThrows(StoragePolicyViolationException.class,
                () -> policy.validate(context(null)));

        assertEquals("CONTENT_TYPE_NOT_ALLOWED", exception.errorCode());
        assertEquals("Content type not allowed: null", exception.getMessage());
    }

    @Test
    void requiresAtLeastOneAllowedContentType() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new AllowlistContentTypePolicy(Set.of("  ")));

        assertEquals("allowedContentTypes must not be empty", exception.getMessage());
    }

    private static UploadContext context(String contentType) {
        return new UploadContext(
                "avatar",
                "bucket",
                "key",
                "avatar.png",
                contentType,
                12L,
                ObjectMetadata.empty()
        );
    }
}
