package io.vacivor.storacle.vendor;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.vacivor.storacle.ObjectStorageClient;
import io.vacivor.storacle.StorageVendorConfig;
import io.vacivor.storacle.StorageVendorOptions;
import io.vacivor.storacle.StorageException;
import io.vacivor.storacle.client.GoogleCloudStorageClient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public final class GoogleCloudObjectStorageClientFactory extends AbstractObjectStorageClientFactory {
    public GoogleCloudObjectStorageClientFactory() {
        super(StorageVendor.GOOGLE_CLOUD);
    }

    @Override
    public ObjectStorageClient create(StorageVendorConfig config) {
        assertVendor(config);

        try {
            StorageOptions.Builder builder = StorageOptions.newBuilder();
            StorageVendorOptions options = config.options();
            String projectId = options.get("projectId");
            if (projectId != null && !projectId.isBlank()) {
                builder.setProjectId(projectId);
            }
            String endpoint = config.endpoint();
            if (endpoint != null && !endpoint.isBlank()) {
                builder.setHost(endpoint);
            }

            GoogleCredentials credentials = resolveCredentials(config);
            if (credentials != null) {
                builder.setCredentials(credentials);
            }

            Storage storage = builder.build().getService();
            return new GoogleCloudStorageClient(storage);
        } catch (IOException e) {
            throw new StorageException("Failed to configure Google Cloud Storage credentials", e);
        }
    }

    private GoogleCredentials resolveCredentials(StorageVendorConfig config) throws IOException {
        StorageVendorOptions options = config.options();
        String credentialsJson = options.get("credentialsJson");
        if (credentialsJson != null && !credentialsJson.isBlank()) {
            InputStream stream = new ByteArrayInputStream(credentialsJson.getBytes(StandardCharsets.UTF_8));
            return GoogleCredentials.fromStream(stream);
        }
        String credentialsPath = options.get("credentialsPath");
        if (credentialsPath != null && !credentialsPath.isBlank()) {
            try (InputStream stream = Files.newInputStream(Path.of(credentialsPath))) {
                return GoogleCredentials.fromStream(stream);
            }
        }
        return null;
    }
}
