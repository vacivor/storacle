package io.vacivor.storacle;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ObjectStorageValueFactoryTest {
    @Test
    void createsObjectSummaryFromFlatValues() {
        ObjectSummary summary = ObjectStorageValueFactory.objectSummary(
                "bucket",
                "file.txt",
                12L,
                Instant.parse("2026-03-12T00:00:00Z"),
                "etag-1"
        );

        assertEquals("bucket", summary.path().bucket());
        assertEquals("file.txt", summary.path().key());
        assertEquals(12L, summary.size());
        assertEquals("etag-1", summary.eTag());
    }

    @Test
    void createsStorageObjectFromComponents() throws Exception {
        StorageObject object = ObjectStorageValueFactory.storageObject(
                ObjectPath.of("bucket", "demo"),
                ObjectMetadata.empty(),
                new ByteArrayInputStream(new byte[]{1, 2, 3})
        );

        assertEquals("bucket", object.path().bucket());
        assertArrayEquals(new byte[]{1, 2, 3}, object.content().readAllBytes());
    }
}
