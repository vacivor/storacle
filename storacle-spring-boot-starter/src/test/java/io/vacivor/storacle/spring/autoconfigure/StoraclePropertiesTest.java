package io.vacivor.storacle.spring.autoconfigure;

import io.vacivor.storacle.StorageVendorConfig;
import io.vacivor.storacle.vendor.StorageVendor;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StoraclePropertiesTest {
    @Test
    void resolveConfigBuildsSelectedVendorConfig() {
        StoracleProperties properties = new StoracleProperties();
        StoracleProperties.VendorProperties vendorProperties = new StoracleProperties.VendorProperties();
        vendorProperties.setEndpoint("http://localhost:9000");
        vendorProperties.setRegion("auto");
        vendorProperties.setAccessKey("ak");
        vendorProperties.setSecretKey("sk");
        vendorProperties.setPathStyleAccess(true);
        vendorProperties.setOptions(Map.of("bucketAcl", "private"));
        properties.setVendor("minio");
        properties.setVendors(Map.of("minio", vendorProperties));

        StorageVendorConfig config = properties.resolveConfig();

        assertEquals(StorageVendor.MINIO, config.vendor());
        assertEquals("http://localhost:9000", config.endpoint());
        assertEquals("auto", config.region());
        assertEquals("ak", config.accessKey());
        assertEquals("sk", config.secretKey());
        assertEquals(true, config.pathStyleAccess());
        assertEquals("private", config.options().get("bucketAcl"));
    }

    @Test
    void resolveConfigRejectsMissingSelectedVendorConfiguration() {
        StoracleProperties properties = new StoracleProperties();
        properties.setVendor("minio");

        assertThrows(IllegalStateException.class, properties::resolveConfig);
    }

    @Test
    void defaultsToDefaultContentTypeDetector() {
        StoracleProperties properties = new StoracleProperties();

        assertEquals(StoracleProperties.ContentTypeDetectorType.DEFAULT, properties.getContentTypeDetector());
    }
}
