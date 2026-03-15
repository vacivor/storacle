package io.vacivor.storacle;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ChecksumUtilsTest {
    @Test
    void hashesTextWithMessageDigestAlgorithms() {
        assertEquals("5d41402abc4b2a76b9719d911017c592", ChecksumUtils.checksum("hello", ChecksumAlgorithm.MD5));
        assertEquals("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                ChecksumUtils.checksum("hello", ChecksumAlgorithm.SHA_256));
    }

    @Test
    void hashesStreamWithChecksumAlgorithms() {
        Map<ChecksumAlgorithm, String> checksums = ChecksumUtils.checksums(
                new ByteArrayInputStream("hello".getBytes()),
                java.util.List.of(ChecksumAlgorithm.CRC32, ChecksumAlgorithm.CRC32C)
        );

        assertEquals("3610a686", checksums.get(ChecksumAlgorithm.CRC32));
        assertEquals("9a71bb4c", checksums.get(ChecksumAlgorithm.CRC32C));
    }
}
