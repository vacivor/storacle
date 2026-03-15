package io.vacivor.storacle;

import org.apache.tika.Tika;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public final class TikaContentTypeDetector implements ContentTypeDetector {
    private final Tika tika;

    public TikaContentTypeDetector() {
        this(new Tika());
    }

    TikaContentTypeDetector(Tika tika) {
        this.tika = Objects.requireNonNull(tika, "tika must not be null");
    }

    @Override
    public String detect(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "application/octet-stream";
        }
        return normalize(tika.detect(bytes));
    }

    @Override
    public String detect(InputStream in, String filename) {
        try {
            return normalize(tika.detect(in, filename));
        } catch (IOException e) {
            throw new StorageException("Failed to detect content type from stream", e);
        }
    }

    @Override
    public String detect(File file) {
        try {
            return normalize(tika.detect(file));
        } catch (IOException e) {
            throw new StorageException("Failed to detect content type for file: " + file.getAbsolutePath(), e);
        }
    }

    @Override
    public String detect(byte[] bytes, String filename) {
        if (bytes == null || bytes.length == 0) {
            return "application/octet-stream";
        }
        return normalize(tika.detect(bytes, filename));
    }

    private static String normalize(String detected) {
        return detected == null || detected.isBlank() ? "application/octet-stream" : detected;
    }
}
