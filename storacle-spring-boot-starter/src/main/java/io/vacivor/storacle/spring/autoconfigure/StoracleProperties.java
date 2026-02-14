package io.vacivor.storacle.spring.autoconfigure;

import io.vacivor.storacle.StorageVendorConfig;
import io.vacivor.storacle.vendor.StorageVendor;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "storacle")
public class StoracleProperties {
    private String vendor = StorageVendor.AMAZON_S3.id();
    private Map<String, VendorProperties> vendors = new HashMap<>();

    public String getVendor() {
        return vendor;
    }

    public void setVendor(String vendor) {
        this.vendor = vendor;
    }

    public Map<String, VendorProperties> getVendors() {
        return vendors;
    }

    public void setVendors(Map<String, VendorProperties> vendors) {
        this.vendors = vendors;
    }

    // Backward compatibility for old key: storacle.provider
    public void setProvider(String provider) {
        this.vendor = provider;
    }

    // Backward compatibility for old key: storacle.providers
    public void setProviders(Map<String, VendorProperties> providers) {
        this.vendors = providers;
    }

    public StorageVendor resolveVendor() {
        return StorageVendor.fromId(vendor);
    }

    public StorageVendorConfig resolveConfig() {
        StorageVendor resolvedVendor = resolveVendor();
        VendorProperties properties = vendors.get(resolvedVendor.id());
        if (properties == null) {
            throw new IllegalStateException("Missing storacle.vendors." + resolvedVendor.id() + " configuration");
        }
        return StorageVendorConfig.builder(resolvedVendor)
                .endpoint(properties.getEndpoint())
                .region(properties.getRegion())
                .accessKey(properties.getAccessKey())
                .secretKey(properties.getSecretKey())
                .pathStyleAccess(properties.getPathStyleAccess())
                .options(properties.getOptions())
                .build();
    }

    public static class VendorProperties {
        private String endpoint;
        private String region;
        private String accessKey;
        private String secretKey;
        private Boolean pathStyleAccess;
        private Map<String, String> options = new HashMap<>();

        public String getEndpoint() {
            return endpoint;
        }

        public void setEndpoint(String endpoint) {
            this.endpoint = endpoint;
        }

        public String getRegion() {
            return region;
        }

        public void setRegion(String region) {
            this.region = region;
        }

        public String getAccessKey() {
            return accessKey;
        }

        public void setAccessKey(String accessKey) {
            this.accessKey = accessKey;
        }

        public String getSecretKey() {
            return secretKey;
        }

        public void setSecretKey(String secretKey) {
            this.secretKey = secretKey;
        }

        public Boolean getPathStyleAccess() {
            return pathStyleAccess;
        }

        public void setPathStyleAccess(Boolean pathStyleAccess) {
            this.pathStyleAccess = pathStyleAccess;
        }

        public Map<String, String> getOptions() {
            return options;
        }

        public void setOptions(Map<String, String> options) {
            this.options = options == null ? new HashMap<>() : new HashMap<>(options);
        }
    }
}
