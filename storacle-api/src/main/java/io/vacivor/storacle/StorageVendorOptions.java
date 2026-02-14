package io.vacivor.storacle;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class StorageVendorOptions {
    private final Map<String, String> values;

    private StorageVendorOptions(Map<String, String> values) {
        this.values = Collections.unmodifiableMap(values);
    }

    public static StorageVendorOptions from(Map<String, String> values) {
        if (values == null || values.isEmpty()) {
            return new StorageVendorOptions(Map.of());
        }
        return new StorageVendorOptions(new HashMap<>(values));
    }

    public Map<String, String> asMap() {
        return values;
    }

    public String get(String key) {
        return values.get(key);
    }

    public String get(String key, String defaultValue) {
        String value = values.get(key);
        return value == null ? defaultValue : value;
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        String value = values.get(key);
        if (value == null) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }

    public int getInt(String key, int defaultValue) {
        String value = values.get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException ex) {
            return defaultValue;
        }
    }

    public long getLong(String key, long defaultValue) {
        String value = values.get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException ex) {
            return defaultValue;
        }
    }
}
