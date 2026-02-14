package io.vacivor.storacle;

import java.time.Instant;

public record FilenameContext(String originalFilename, String extension, String prefix, String contentType,
                              Instant timestamp) {
    public static FilenameContext of(String originalFilename, String prefix, String contentType) {
        String extension = null;
        if (originalFilename != null) {
            int dot = originalFilename.lastIndexOf('.');
            if (dot > -1 && dot < originalFilename.length() - 1) {
                extension = originalFilename.substring(dot);
            }
        }
        return new FilenameContext(originalFilename, extension, prefix, contentType, Instant.now());
    }
}
