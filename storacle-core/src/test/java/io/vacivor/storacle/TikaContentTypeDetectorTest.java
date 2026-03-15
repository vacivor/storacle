package io.vacivor.storacle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TikaContentTypeDetectorTest {
    private final TikaContentTypeDetector detector = new TikaContentTypeDetector();

    @Test
    void detectsPngFromMagicBytes() {
        byte[] png = new byte[] {
                (byte) 0x89, 'P', 'N', 'G', '\r', '\n', 0x1A, '\n'
        };

        assertEquals("image/png", detector.detect(png, "image.bin"));
    }

    @Test
    void fallsBackToFilenameWhenBytesAreMissing() {
        assertEquals("application/octet-stream", detector.detect((byte[]) null, "note.txt"));
    }

    @Test
    void detectsInputStreamUsingFilenameFallback() {
        assertEquals("text/plain",
                detector.detect(new ByteArrayInputStream("hello".getBytes()), "note.txt"));
    }

    @Test
    void detectsFile(@TempDir Path tempDir) throws Exception {
        Path file = tempDir.resolve("note.txt");
        Files.writeString(file, "hello");

        assertEquals("text/plain", detector.detect(file.toFile()));
    }
}
