package io.vacivor.storacle;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ObjectWriteResultTest {
    @Test
    void returnsChecksumForAlgorithm() {
        ObjectWriteResult result = new ObjectWriteResult(
                ObjectPath.of("bucket", "key"),
                "file.txt",
                "text/plain",
                5L,
                "etag",
                null,
                Map.of(ChecksumAlgorithm.CRC32, "3610a686")
        );

        assertEquals("3610a686", result.checksum(ChecksumAlgorithm.CRC32));
    }

    @Test
    void returnsNullWhenChecksumMissing() {
        ObjectWriteResult result = new ObjectWriteResult(ObjectPath.of("bucket", "key"), "etag", null);

        assertNull(result.checksumOrNull(ChecksumAlgorithm.CRC32));
    }

    @Test
    void rejectsMissingChecksumInStrictAccessor() {
        ObjectWriteResult result = new ObjectWriteResult(ObjectPath.of("bucket", "key"), "etag", null);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> result.checksum(ChecksumAlgorithm.CRC32));

        assertEquals("Missing checksum for algorithm: CRC32", exception.getMessage());
    }
}
