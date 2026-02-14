package io.vacivor.storacle.vendor;

import io.vacivor.storacle.ObjectStorageClient;
import io.vacivor.storacle.StorageVendorConfig;
import io.vacivor.storacle.StorageException;
import io.vacivor.storacle.client.AmazonS3StorageClient;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;

public final class AmazonS3ObjectStorageClientFactory extends AbstractObjectStorageClientFactory {
    public AmazonS3ObjectStorageClientFactory() {
        super(StorageVendor.AMAZON_S3);
    }

    @Override
    public ObjectStorageClient create(StorageVendorConfig config) {
        assertVendor(config);

        String accessKey = requireNonBlank(config.accessKey(), "accessKey");
        String secretKey = requireNonBlank(config.secretKey(), "secretKey");
        String region = requireNonBlank(config.region(), "region");

        AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
        S3Configuration s3Configuration = S3Configuration.builder()
                .pathStyleAccessEnabled(Boolean.TRUE.equals(config.pathStyleAccess()))
                .build();

        S3ClientBuilder builder = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .serviceConfiguration(s3Configuration)
                .region(Region.of(region));

        if (config.endpoint() != null && !config.endpoint().isBlank()) {
            try {
                builder.endpointOverride(URI.create(config.endpoint()));
            } catch (IllegalArgumentException ex) {
                throw new StorageException("Invalid endpoint: " + config.endpoint(), ex);
            }
        }

        return new AmazonS3StorageClient(builder.build());
    }

    private static String requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }
}
