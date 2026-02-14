package io.vacivor.storacle.client;

import java.util.Objects;

import io.vacivor.storacle.ListObjectsPage;
import io.vacivor.storacle.ListObjectsRequest;
import io.vacivor.storacle.ObjectMetadata;
import io.vacivor.storacle.ObjectPath;
import io.vacivor.storacle.ObjectStorageClient;
import io.vacivor.storacle.ObjectSummary;
import io.vacivor.storacle.ObjectWriteResult;
import io.vacivor.storacle.StorageException;
import io.vacivor.storacle.StorageObject;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

public final class AmazonS3StorageClient implements ObjectStorageClient {
    private final S3Client client;

    public AmazonS3StorageClient(S3Client client) {
        this.client = Objects.requireNonNull(client, "client must not be null");
    }

    @Override
    public ObjectWriteResult put(ObjectPath path, InputStream content, ObjectMetadata metadata) {
        Objects.requireNonNull(path, "path must not be null");
        Objects.requireNonNull(content, "content must not be null");
        ObjectMetadata safeMetadata = metadata == null ? ObjectMetadata.empty() : metadata;

        try {
            PutObjectRequest.Builder builder = PutObjectRequest.builder()
                    .bucket(path.bucket())
                    .key(path.key());

            if (safeMetadata.contentType() != null) {
                builder.contentType(safeMetadata.contentType());
            }
            if (safeMetadata.cacheControl() != null) {
                builder.cacheControl(safeMetadata.cacheControl());
            }
            if (safeMetadata.contentDisposition() != null) {
                builder.contentDisposition(safeMetadata.contentDisposition());
            }
            if (!safeMetadata.userMetadata().isEmpty()) {
                builder.metadata(safeMetadata.userMetadata());
            }

            PutObjectRequest request = builder.build();
            RequestBody body = toRequestBody(content, safeMetadata);
            PutObjectResponse response = client.putObject(request, body);
            return new ObjectWriteResult(path, response.eTag(), response.versionId());
        } catch (SdkException e) {
            throw new StorageException("Failed to put object: " + path.bucket() + "/" + path.key(), e);
        }
    }

    @Override
    public StorageObject get(ObjectPath path) {
        Objects.requireNonNull(path, "path must not be null");
        try {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(path.bucket())
                    .key(path.key())
                    .build();
            ResponseInputStream<GetObjectResponse> response = client.getObject(request);
            GetObjectResponse meta = response.response();
            ObjectMetadata metadata = ObjectMetadata.builder()
                    .contentType(meta.contentType())
                    .contentLength(meta.contentLength())
                    .cacheControl(meta.cacheControl())
                    .contentDisposition(meta.contentDisposition())
                    .userMetadata(meta.metadata())
                    .build();
            return new StorageObject(path, metadata, response);
        } catch (SdkException e) {
            throw new StorageException("Failed to get object: " + path.bucket() + "/" + path.key(), e);
        }
    }

    @Override
    public boolean delete(ObjectPath path) {
        Objects.requireNonNull(path, "path must not be null");
        try {
            DeleteObjectRequest request = DeleteObjectRequest.builder()
                    .bucket(path.bucket())
                    .key(path.key())
                    .build();
            client.deleteObject(request);
            return true;
        } catch (SdkException e) {
            throw new StorageException("Failed to delete object: " + path.bucket() + "/" + path.key(), e);
        }
    }

    @Override
    public boolean exists(ObjectPath path) {
        Objects.requireNonNull(path, "path must not be null");
        try {
            HeadObjectRequest request = HeadObjectRequest.builder()
                    .bucket(path.bucket())
                    .key(path.key())
                    .build();
            client.headObject(request);
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return false;
            }
            throw new StorageException("Failed to check object existence: " + path.bucket() + "/" + path.key(), e);
        } catch (SdkException e) {
            throw new StorageException("Failed to check object existence: " + path.bucket() + "/" + path.key(), e);
        }
    }

    @Override
    public ListObjectsPage list(ListObjectsRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        try {
            ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder()
                    .bucket(request.bucket())
                    .maxKeys(request.maxKeys());
            if (request.prefix() != null) {
                builder.prefix(request.prefix());
            }
            if (request.continuationToken() != null) {
                builder.continuationToken(request.continuationToken());
            }

            ListObjectsV2Response response = client.listObjectsV2(builder.build());
            List<ObjectSummary> summaries = response.contents().stream()
                    .map(obj -> new ObjectSummary(
                            ObjectPath.of(request.bucket(), obj.key()),
                            obj.size(),
                            obj.lastModified(),
                            obj.eTag()))
                    .toList();
            return new ListObjectsPage(summaries, response.nextContinuationToken(), response.isTruncated());
        } catch (SdkException e) {
            throw new StorageException("Failed to list objects for bucket: " + request.bucket(), e);
        }
    }

    @Override
    public ObjectWriteResult copy(ObjectPath source, ObjectPath target, ObjectMetadata metadataOverride) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(target, "target must not be null");
        try {
            CopyObjectRequest.Builder builder = CopyObjectRequest.builder()
                    .copySource(encodeCopySource(source))
                    .destinationBucket(target.bucket())
                    .destinationKey(target.key());

            if (metadataOverride != null) {
                builder.metadataDirective(MetadataDirective.REPLACE);
                if (metadataOverride.contentType() != null) {
                    builder.contentType(metadataOverride.contentType());
                }
                if (metadataOverride.cacheControl() != null) {
                    builder.cacheControl(metadataOverride.cacheControl());
                }
                if (metadataOverride.contentDisposition() != null) {
                    builder.contentDisposition(metadataOverride.contentDisposition());
                }
                if (!metadataOverride.userMetadata().isEmpty()) {
                    builder.metadata(metadataOverride.userMetadata());
                }
            } else {
                builder.metadataDirective(MetadataDirective.COPY);
            }

            CopyObjectResponse response = client.copyObject(builder.build());
            String eTag = response.copyObjectResult() == null ? null : response.copyObjectResult().eTag();
            return new ObjectWriteResult(target, eTag, response.versionId());
        } catch (SdkException e) {
            throw new StorageException("Failed to copy object: " + source.bucket() + "/" + source.key(), e);
        }
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public <T> T unwrap(Class<T> type) {
        if (type.isInstance(client)) {
            return type.cast(client);
        }
        return ObjectStorageClient.super.unwrap(type);
    }

    private static RequestBody toRequestBody(InputStream content, ObjectMetadata metadata) {
        if (metadata.contentLength() != null && metadata.contentLength() >= 0) {
            return RequestBody.fromInputStream(content, metadata.contentLength());
        }
        try {
            byte[] bytes = content.readAllBytes();
            return RequestBody.fromBytes(bytes);
        } catch (IOException e) {
            throw new StorageException("Failed to read content", e);
        }
    }

    private static String encodeCopySource(ObjectPath path) {
        String encodedBucket = URLEncoder.encode(path.bucket(), StandardCharsets.UTF_8)
                .replace("+", "%20");
        String encodedKey = URLEncoder.encode(path.key(), StandardCharsets.UTF_8)
                .replace("+", "%20");
        return encodedBucket + "/" + encodedKey;
    }
}
