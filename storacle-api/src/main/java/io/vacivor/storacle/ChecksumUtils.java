package io.vacivor.storacle;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

public final class ChecksumUtils {
    private static final int DEFAULT_BUFFER_SIZE = 8192;

    private ChecksumUtils() {
    }

    public static String checksum(String value, ChecksumAlgorithm algorithm) {
        return checksum(value, StandardCharsets.UTF_8, algorithm);
    }

    public static String checksum(String value, Charset charset, ChecksumAlgorithm algorithm) {
        Objects.requireNonNull(value, "value must not be null");
        Objects.requireNonNull(charset, "charset must not be null");
        return checksum(value.getBytes(charset), algorithm);
    }

    public static String checksum(byte[] value, ChecksumAlgorithm algorithm) {
        Objects.requireNonNull(value, "value must not be null");
        Objects.requireNonNull(algorithm, "algorithm must not be null");
        ChecksumAlgorithm.ChecksumAccumulator accumulator = algorithm.newAccumulator();
        accumulator.update(value, 0, value.length);
        return accumulator.hex();
    }

    public static String checksum(InputStream input, ChecksumAlgorithm algorithm) {
        return checksums(input, java.util.List.of(algorithm)).get(algorithm);
    }

    public static Map<ChecksumAlgorithm, String> checksums(
            InputStream input, Collection<ChecksumAlgorithm> algorithms) {
        Objects.requireNonNull(input, "input must not be null");
        Objects.requireNonNull(algorithms, "algorithms must not be null");
        ChecksumInputStream checksumInputStream = new ChecksumInputStream(input, algorithms);
        byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
        try (checksumInputStream) {
            while (checksumInputStream.read(buffer) >= 0) {
                // Read to EOF so every accumulator sees the full stream.
            }
            return checksumInputStream.checksums();
        } catch (IOException e) {
            throw new StorageException("Failed to calculate content checksum", e);
        }
    }

}
