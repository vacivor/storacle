package io.vacivor.storacle;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ChecksumMetadataTest {
    @Test
    void extractsChecksumsFromUserMetadata() {
        Map<ChecksumAlgorithm, String> checksums = ChecksumMetadata.extract(Map.of(
                "checksum-sha-256", "abc",
                "checksum-crc32", "def",
                "owner", "storacle"
        ));

        assertEquals(Map.of(
                ChecksumAlgorithm.SHA_256, "abc",
                ChecksumAlgorithm.CRC32, "def"
        ), checksums);
    }

    @Test
    void mergesChecksumsIntoMetadata() {
        ObjectMetadata metadata = ObjectMetadata.builder()
                .putUserMetadata("owner", "storacle")
                .build();

        ObjectMetadata merged = ChecksumMetadata.withChecksums(metadata, Map.of(
                ChecksumAlgorithm.SHA_256, "abc123"
        ));

        assertEquals("storacle", merged.userMetadata().get("owner"));
        assertEquals("abc123", merged.userMetadata().get("checksum-sha-256"));
    }
}
