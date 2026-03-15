package io.vacivor.storacle.template;

import io.vacivor.storacle.ObjectMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CompositeContentTypePolicyTest {
    @Test
    void validatesPoliciesInOrder() {
        AtomicInteger calls = new AtomicInteger();
        CompositeContentTypePolicy policy = new CompositeContentTypePolicy(List.of(
                context -> calls.incrementAndGet(),
                context -> calls.incrementAndGet()
        ));

        policy.validate(context());

        assertEquals(2, calls.get());
    }

    @Test
    void requiresAtLeastOnePolicy() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new CompositeContentTypePolicy(List.of()));

        assertEquals("policies must not be empty", exception.getMessage());
    }

    private static UploadContext context() {
        return new UploadContext("scene", "bucket", "key", "file.txt", "text/plain", 5L, ObjectMetadata.empty());
    }
}
