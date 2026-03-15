package io.vacivor.storacle;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.CRC32;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

public enum ChecksumAlgorithm {
    MD5("MD5"),
    SHA_1("SHA-1"),
    SHA_256("SHA-256"),
    SHA_384("SHA-384"),
    SHA_512("SHA-512"),
    CRC32("CRC32"),
    CRC32C("CRC32C");

    private final String value;

    ChecksumAlgorithm(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    public static ChecksumAlgorithm from(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("hash algorithm must not be blank");
        }
        for (ChecksumAlgorithm algorithm : values()) {
            if (algorithm.value.equalsIgnoreCase(value) || algorithm.name().equalsIgnoreCase(value)) {
                return algorithm;
            }
        }
        throw new IllegalArgumentException("Unsupported hash algorithm: " + value);
    }

    public ChecksumAccumulator newAccumulator() {
        return switch (this) {
            case MD5, SHA_1, SHA_256, SHA_384, SHA_512 -> new MessageDigestChecksumAccumulator(this, value);
            case CRC32 -> new CyclicRedundancyChecksumAccumulator(this, new CRC32());
            case CRC32C -> new CyclicRedundancyChecksumAccumulator(this, new CRC32C());
        };
    }

    public interface ChecksumAccumulator {
        ChecksumAlgorithm algorithm();

        void update(byte[] buffer, int offset, int length);

        String hex();
    }

    private static final class MessageDigestChecksumAccumulator implements ChecksumAccumulator {
        private final ChecksumAlgorithm algorithm;
        private final MessageDigest digest;

        private MessageDigestChecksumAccumulator(ChecksumAlgorithm algorithm, String name) {
            this.algorithm = algorithm;
            try {
                this.digest = MessageDigest.getInstance(name);
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("JVM does not support hash algorithm: " + name, e);
            }
        }

        @Override
        public ChecksumAlgorithm algorithm() {
            return algorithm;
        }

        @Override
        public void update(byte[] buffer, int offset, int length) {
            digest.update(buffer, offset, length);
        }

        @Override
        public String hex() {
            return toHex(digest.digest());
        }
    }

    private static final class CyclicRedundancyChecksumAccumulator implements ChecksumAccumulator {
        private final ChecksumAlgorithm algorithm;
        private final Checksum checksum;

        private CyclicRedundancyChecksumAccumulator(ChecksumAlgorithm algorithm, Checksum checksum) {
            this.algorithm = algorithm;
            this.checksum = checksum;
        }

        @Override
        public ChecksumAlgorithm algorithm() {
            return algorithm;
        }

        @Override
        public void update(byte[] buffer, int offset, int length) {
            checksum.update(buffer, offset, length);
        }

        @Override
        public String hex() {
            long value = checksum.getValue();
            return String.format("%08x", value);
        }
    }

    private static String toHex(byte[] value) {
        StringBuilder builder = new StringBuilder(value.length * 2);
        for (byte b : value) {
            builder.append(Character.forDigit((b >>> 4) & 0xF, 16));
            builder.append(Character.forDigit(b & 0xF, 16));
        }
        return builder.toString();
    }
}
