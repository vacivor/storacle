package io.vacivor.storacle;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLConnection;

public class DefaultContentTypeDetector implements ContentTypeDetector {
    private static final int SNIFF_BYTES = 8192;

    @Override
    public String detect(byte[] bytes) {
        String detected = null;
        if (bytes != null && bytes.length > 0) {
            try {
                detected = URLConnection.guessContentTypeFromStream(new ByteArrayInputStream(bytes));
            } catch (IOException ignored) {
                // Ignore and fall back to default.
            }
        }
        return detected != null ? detected : "application/octet-stream";
    }

    @Override
    public String detect(InputStream in, String filename) {
        if (in == null) {
            return "application/octet-stream";
        }
        try {
            byte[] prefix = readPrefix(in, SNIFF_BYTES);
            return detect(prefix);
        } catch (IOException e) {
            throw new StorageException("Failed to detect content type from stream", e);
        }
    }

    @Override
    public String detect(File file) {
        if (file == null) {
            return "application/octet-stream";
        }
        try {
            String detected = java.nio.file.Files.probeContentType(file.toPath());
            return detected == null || detected.isBlank() ? "application/octet-stream" : detected;
        } catch (IOException e) {
            throw new StorageException("Failed to detect content type for file: " + file.getAbsolutePath(), e);
        }
    }

    @Override
    public String detect(byte[] bytes, String filename) {
        return detect(bytes);
    }

    private static byte[] readPrefix(InputStream input, int maxBytes) throws IOException {
        if (input.markSupported()) {
            input.mark(maxBytes);
            byte[] prefix = input.readNBytes(maxBytes);
            input.reset();
            return prefix;
        }
        return input.readNBytes(maxBytes);
    }
}
