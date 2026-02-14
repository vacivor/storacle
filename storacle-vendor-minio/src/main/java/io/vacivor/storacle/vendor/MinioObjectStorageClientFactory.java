package io.vacivor.storacle.vendor;

import io.minio.MinioClient;
import io.vacivor.storacle.ObjectStorageClient;
import io.vacivor.storacle.StorageVendorConfig;
import io.vacivor.storacle.client.MinioStorageClient;

public final class MinioObjectStorageClientFactory extends AbstractObjectStorageClientFactory {
    public MinioObjectStorageClientFactory() {
        super(StorageVendor.MINIO);
    }

    @Override
    public ObjectStorageClient create(StorageVendorConfig config) {
        assertVendor(config);

        String endpoint = config.endpoint();
        if (endpoint == null || endpoint.isBlank()) {
            endpoint = "http://localhost:9000";
        }
        String accessKey = requireNonBlank(config.accessKey(), "accessKey");
        String secretKey = requireNonBlank(config.secretKey(), "secretKey");

        MinioClient client = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey, secretKey)
                .build();

        return new MinioStorageClient(client);
    }

    private static String requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }
}
