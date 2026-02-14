package io.vacivor.storacle;

import io.vacivor.storacle.vendor.StorageVendor;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class StorageVendorConfig {
    private final StorageVendor vendor;
    private final String endpoint;
    private final String region;
    private final String accessKey;
    private final String secretKey;
    private final Boolean pathStyleAccess;
    private final StorageVendorOptions options;

    private StorageVendorConfig(Builder builder) {
        this.vendor = Objects.requireNonNull(builder.vendor, "vendor must not be null");
        this.endpoint = builder.endpoint;
        this.region = builder.region;
        this.accessKey = builder.accessKey;
        this.secretKey = builder.secretKey;
        this.pathStyleAccess = builder.pathStyleAccess;
        this.options = StorageVendorOptions.from(builder.options);
    }

    public static Builder builder(StorageVendor vendor) {
        return new Builder(vendor);
    }

    public StorageVendor vendor() {
        return vendor;
    }

    public String endpoint() {
        return endpoint;
    }

    public String region() {
        return region;
    }

    public String accessKey() {
        return accessKey;
    }

    public String secretKey() {
        return secretKey;
    }

    public Boolean pathStyleAccess() {
        return pathStyleAccess;
    }

    public StorageVendorOptions options() {
        return options;
    }

    public static final class Builder {
        private final StorageVendor vendor;
        private String endpoint;
        private String region;
        private String accessKey;
        private String secretKey;
        private Boolean pathStyleAccess;
        private Map<String, String> options = new HashMap<>();

        private Builder(StorageVendor vendor) {
            this.vendor = Objects.requireNonNull(vendor, "vendor");
        }

        public Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder region(String region) {
            this.region = region;
            return this;
        }

        public Builder accessKey(String accessKey) {
            this.accessKey = accessKey;
            return this;
        }

        public Builder secretKey(String secretKey) {
            this.secretKey = secretKey;
            return this;
        }

        public Builder pathStyleAccess(Boolean pathStyleAccess) {
            this.pathStyleAccess = pathStyleAccess;
            return this;
        }

        public Builder options(Map<String, String> options) {
            this.options = (options == null) ? new HashMap<>() : new HashMap<>(options);
            return this;
        }

        public Builder option(String key, String value) {
            if (key != null && value != null) {
                this.options.put(key, value);
            }
            return this;
        }

        public StorageVendorConfig build() {
            return new StorageVendorConfig(this);
        }
    }
}
