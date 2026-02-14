package io.vacivor.storacle;


public record ObjectPath(String bucket, String key) {
    public ObjectPath {
        bucket = requireNonBlank(bucket, "bucket");
        key = requireNonBlank(key, "key");
    }

    public static ObjectPath of(String bucket, String key) {
        return new ObjectPath(bucket, key);
    }

    private static String requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }
}
