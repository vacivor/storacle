package io.vacivor.storacle.vendor;

import java.util.Objects;

import io.vacivor.storacle.StorageVendorConfig;

public abstract class AbstractObjectStorageClientFactory implements ObjectStorageClientFactory {
    private final StorageVendor vendor;

    protected AbstractObjectStorageClientFactory(StorageVendor vendor) {
        this.vendor = Objects.requireNonNull(vendor, "vendor must not be null");
    }

    @Override
    public StorageVendor vendor() {
        return vendor;
    }

    protected void assertVendor(StorageVendorConfig config) {
        Objects.requireNonNull(config, "config must not be null");
        if (config.vendor() != vendor) {
            throw new IllegalArgumentException("Vendor mismatch. Expected " + vendor.id() + " but got " + config.vendor().id());
        }
    }

    protected final String requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    protected final boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
