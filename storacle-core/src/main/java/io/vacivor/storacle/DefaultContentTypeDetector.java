package io.vacivor.storacle;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLConnection;
import java.nio.file.Files;

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
            return fallbackByFilename(filename);
        }
        String detected = null;
        try {
            byte[] prefix = readPrefix(in, SNIFF_BYTES);
            detected = detect(prefix);
        } catch (IOException ignored) {
            // Ignore and fall back to filename-based detection.
        }
        if ("application/octet-stream".equals(detected)) {
            return fallbackByFilename(filename);
        }
        return detected;
    }

    @Override
    public String detect(File file) {
        if (file == null) {
            return "application/octet-stream";
        }
        try {
            String detected = Files.probeContentType(file.toPath());
            if (detected != null && !detected.isBlank()) {
                return detected;
            }
        } catch (IOException ignored) {
            // Ignore and fall back.
        }
        try (InputStream input = new FileInputStream(file)) {
            return detect(input, file.getName());
        } catch (IOException ignored) {
            return fallbackByFilename(file.getName());
        }
    }

    @Override
    public String detect(byte[] bytes, String filename) {
        String detected = detect(bytes);
        if (!"application/octet-stream".equals(detected)) {
            return detected;
        }
        return fallbackByFilename(filename);
    }

    private static String fallbackByFilename(String filename) {
        String detected = null;
        if (filename != null) {
            detected = URLConnection.guessContentTypeFromName(filename);
        }
        return detected != null ? detected : "application/octet-stream";
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
