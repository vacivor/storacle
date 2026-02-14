package io.vacivor.storacle;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class DatePathFilenameGenerator implements FilenameGenerator {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy/MM/dd").withZone(ZoneOffset.UTC);

    @Override
    public String generate(FilenameContext context) {
        String extension = context.extension() == null ? "" : context.extension();
        String base = UUID.randomUUID().toString().replace("-", "");
        String datePath = FORMATTER.format(context.timestamp());
        return join(context.prefix(), datePath + "/" + base + extension);
    }

    private static String join(String prefix, String name) {
        if (prefix == null || prefix.isBlank()) {
            return name;
        }
        String trimmed = trimSlashes(prefix);
        return trimmed + "/" + name;
    }

    private static String trimSlashes(String value) {
        int start = 0;
        int end = value.length();
        while (start < end && value.charAt(start) == '/') {
            start++;
        }
        while (end > start && value.charAt(end - 1) == '/') {
            end--;
        }
        return value.substring(start, end);
    }
}
