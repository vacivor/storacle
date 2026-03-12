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
        this.vendors = vendors == null ? new HashMap<>() : new HashMap<>(vendors);
    }

    public StorageVendorConfig resolveConfig() {
        StorageVendor resolvedVendor = StorageVendor.fromId(vendor);
        VendorProperties properties = requireVendorProperties(resolvedVendor);
        return properties.toConfig(resolvedVendor);
    }

    private VendorProperties requireVendorProperties(StorageVendor vendor) {
        VendorProperties properties = vendors.get(vendor.id());
        if (properties == null) {
            throw new IllegalStateException("Missing storacle.vendors." + vendor.id() + " configuration");
        }
        return properties;
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

        private StorageVendorConfig toConfig(StorageVendor vendor) {
            return StorageVendorConfig.builder(vendor)
                    .endpoint(endpoint)
                    .region(region)
                    .accessKey(accessKey)
                    .secretKey(secretKey)
                    .pathStyleAccess(pathStyleAccess)
                    .options(options)
                    .build();
        }
    }
}
