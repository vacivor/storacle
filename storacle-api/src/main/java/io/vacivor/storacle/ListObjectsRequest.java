package io.vacivor.storacle;


public final class ListObjectsRequest {
    private final String bucket;
    private final String prefix;
    private final int maxKeys;
    private final String continuationToken;

    private ListObjectsRequest(Builder builder) {
        this.bucket = requireNonBlank(builder.bucket, "bucket");
        this.prefix = builder.prefix;
        this.maxKeys = requirePositive(builder.maxKeys, "maxKeys");
        this.continuationToken = builder.continuationToken;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String bucket() {
        return bucket;
    }

    public String prefix() {
        return prefix;
    }

    public int maxKeys() {
        return maxKeys;
    }

    public String continuationToken() {
        return continuationToken;
    }

    public static final class Builder {
        private String bucket;
        private String prefix;
        private int maxKeys = 1000;
        private String continuationToken;

        public Builder bucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder prefix(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public Builder maxKeys(int maxKeys) {
            this.maxKeys = maxKeys;
            return this;
        }

        public Builder continuationToken(String continuationToken) {
            this.continuationToken = continuationToken;
            return this;
        }

        public ListObjectsRequest build() {
            return new ListObjectsRequest(this);
        }
    }

    private static String requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    private static int requirePositive(int value, String field) {
        if (value <= 0) {
            throw new IllegalArgumentException(field + " must be > 0");
        }
        return value;
    }
}
