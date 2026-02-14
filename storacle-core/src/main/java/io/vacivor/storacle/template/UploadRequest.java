package io.vacivor.storacle.template;

import java.util.Objects;

import io.vacivor.storacle.ObjectMetadata;

import java.io.InputStream;

public final class UploadRequest {
    private final String bucket;
    private final String objectKey;
    private final String originalFilename;
    private final String prefix;
    private final InputStream content;
    private final long contentLength;
    private final ObjectMetadata metadata;
    private final int peekBytes;

    private UploadRequest(Builder builder) {
        this.bucket = requireNonBlank(builder.bucket, "bucket");
        this.objectKey = builder.objectKey;
        this.originalFilename = builder.originalFilename;
        this.prefix = builder.prefix;
        this.content = Objects.requireNonNull(builder.content, "content must not be null");
        if (builder.contentLength < -1) {
            throw new IllegalArgumentException("contentLength must be >= -1");
        }
        this.contentLength = builder.contentLength;
        this.metadata = builder.metadata;
        this.peekBytes = requirePositive(builder.peekBytes, "peekBytes");
    }

    public static Builder builder() {
        return new Builder();
    }

    public String bucket() {
        return bucket;
    }

    public String objectKey() {
        return objectKey;
    }

    public String originalFilename() {
        return originalFilename;
    }

    public String prefix() {
        return prefix;
    }

    public InputStream content() {
        return content;
    }

    public long contentLength() {
        return contentLength;
    }

    public ObjectMetadata metadata() {
        return metadata;
    }

    public int peekBytes() {
        return peekBytes;
    }

    public static final class Builder {
        private String bucket;
        private String objectKey;
        private String originalFilename;
        private String prefix;
        private InputStream content;
        private long contentLength = -1;
        private ObjectMetadata metadata;
        private int peekBytes = 8192;

        public Builder bucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder objectKey(String objectKey) {
            this.objectKey = objectKey;
            return this;
        }

        public Builder originalFilename(String originalFilename) {
            this.originalFilename = originalFilename;
            return this;
        }

        public Builder prefix(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public Builder content(InputStream content) {
            this.content = content;
            return this;
        }

        public Builder contentLength(long contentLength) {
            this.contentLength = contentLength;
            return this;
        }

        public Builder metadata(ObjectMetadata metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder peekBytes(int peekBytes) {
            this.peekBytes = peekBytes;
            return this;
        }

        public UploadRequest build() {
            return new UploadRequest(this);
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
