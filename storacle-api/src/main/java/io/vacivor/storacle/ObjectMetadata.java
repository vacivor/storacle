package io.vacivor.storacle;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class ObjectMetadata {
    private final String contentType;
    private final Long contentLength;
    private final String contentDisposition;
    private final String cacheControl;
    private final Map<String, String> userMetadata;

    private ObjectMetadata(Builder builder) {
        this.contentType = builder.contentType;
        this.contentLength = builder.contentLength;
        this.contentDisposition = builder.contentDisposition;
        this.cacheControl = builder.cacheControl;
        this.userMetadata = Collections.unmodifiableMap(new HashMap<>(builder.userMetadata));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(ObjectMetadata base) {
        return new Builder(base);
    }

    public static ObjectMetadata empty() {
        return builder().build();
    }

    public String contentType() {
        return contentType;
    }

    public Long contentLength() {
        return contentLength;
    }

    public String contentDisposition() {
        return contentDisposition;
    }

    public String cacheControl() {
        return cacheControl;
    }

    public Map<String, String> userMetadata() {
        return userMetadata;
    }

    public static final class Builder {
        private String contentType;
        private Long contentLength;
        private String contentDisposition;
        private String cacheControl;
        private Map<String, String> userMetadata = new HashMap<>();

        private Builder() {
        }

        private Builder(ObjectMetadata base) {
            Objects.requireNonNull(base, "base");
            this.contentType = base.contentType;
            this.contentLength = base.contentLength;
            this.contentDisposition = base.contentDisposition;
            this.cacheControl = base.cacheControl;
            this.userMetadata = new HashMap<>(base.userMetadata);
        }

        public Builder contentType(String contentType) {
            this.contentType = contentType;
            return this;
        }

        public Builder contentLength(Long contentLength) {
            this.contentLength = contentLength;
            return this;
        }

        public Builder contentDisposition(String contentDisposition) {
            this.contentDisposition = contentDisposition;
            return this;
        }

        public Builder cacheControl(String cacheControl) {
            this.cacheControl = cacheControl;
            return this;
        }

        public Builder userMetadata(Map<String, String> userMetadata) {
            this.userMetadata = (userMetadata == null) ? new HashMap<>() : new HashMap<>(userMetadata);
            return this;
        }

        public Builder putUserMetadata(String key, String value) {
            if (key != null && value != null) {
                this.userMetadata.put(key, value);
            }
            return this;
        }

        public ObjectMetadata build() {
            return new ObjectMetadata(this);
        }
    }
}
