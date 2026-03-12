package io.vacivor.storacle.client;

import io.vacivor.storacle.AbstractObjectStorageClient;
import io.vacivor.storacle.ListObjectsPage;
import io.vacivor.storacle.ListObjectsRequest;
import io.vacivor.storacle.ObjectMetadata;
import io.vacivor.storacle.ObjectMetadataMapper;
import io.vacivor.storacle.ObjectPath;
import io.vacivor.storacle.ObjectStorageValueFactory;
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

public final class AmazonS3StorageClient extends AbstractObjectStorageClient {
    private final S3Client client;

    public AmazonS3StorageClient(S3Client client) {
        this.client = java.util.Objects.requireNonNull(client, "client must not be null");
    }

    @Override
    public ObjectWriteResult put(ObjectPath path, InputStream content, ObjectMetadata metadata) {
        path = requirePath(path);
        content = requireContent(content);
        ObjectMetadata safeMetadata = safeMetadata(metadata);

        try {
            PutObjectRequest.Builder builder = PutObjectRequest.builder()
                    .bucket(path.bucket())
                    .key(path.key());
            applyMetadata(builder, safeMetadata);

            PutObjectRequest request = builder.build();
            RequestBody body = toRequestBody(content, safeMetadata);
            PutObjectResponse response = client.putObject(request, body);
            return new ObjectWriteResult(path, response.eTag(), response.versionId());
        } catch (SdkException e) {
            throw objectFailure("put", path, e);
        }
    }

    @Override
    public StorageObject get(ObjectPath path) {
        path = requirePath(path);
        try {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(path.bucket())
                    .key(path.key())
                    .build();
            ResponseInputStream<GetObjectResponse> response = client.getObject(request);
            GetObjectResponse meta = response.response();
            ObjectMetadata metadata = ObjectMetadataMapper.from(
                    meta::contentType,
                    meta::contentLength,
                    meta::cacheControl,
                    meta::contentDisposition,
                    meta::metadata
            );
            return ObjectStorageValueFactory.storageObject(path, metadata, response);
        } catch (SdkException e) {
            throw objectFailure("get", path, e);
        }
    }

    @Override
    public boolean delete(ObjectPath path) {
        path = requirePath(path);
        try {
            DeleteObjectRequest request = DeleteObjectRequest.builder()
                    .bucket(path.bucket())
                    .key(path.key())
                    .build();
            client.deleteObject(request);
            return true;
        } catch (SdkException e) {
            throw objectFailure("delete", path, e);
        }
    }

    @Override
    public boolean exists(ObjectPath path) {
        path = requirePath(path);
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
            throw objectFailure("check object existence", path, e);
        } catch (SdkException e) {
            throw objectFailure("check object existence", path, e);
        }
    }

    @Override
    public ListObjectsPage list(ListObjectsRequest request) {
        ListObjectsRequest validatedRequest = requireListRequest(request);
        try {
            ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder()
                    .bucket(validatedRequest.bucket())
                    .maxKeys(validatedRequest.maxKeys());
            if (validatedRequest.prefix() != null) {
                builder.prefix(validatedRequest.prefix());
            }
            if (validatedRequest.continuationToken() != null) {
                builder.continuationToken(validatedRequest.continuationToken());
            }

            ListObjectsV2Response response = client.listObjectsV2(builder.build());
            List<ObjectSummary> summaries = response.contents().stream()
                    .map(obj -> ObjectStorageValueFactory.objectSummary(
                            validatedRequest.bucket(),
                            obj.key(),
                            obj.size(),
                            obj.lastModified(),
                            obj.eTag()))
                    .toList();
            return new ListObjectsPage(summaries, response.nextContinuationToken(), response.isTruncated());
        } catch (SdkException e) {
            throw bucketFailure("list", validatedRequest.bucket(), e);
        }
    }

    @Override
    public ObjectWriteResult copy(ObjectPath source, ObjectPath target, ObjectMetadata metadataOverride) {
        source = requirePath(source);
        target = requirePath(target);
        try {
            CopyObjectRequest.Builder builder = CopyObjectRequest.builder()
                    .copySource(encodeCopySource(source))
                    .destinationBucket(target.bucket())
                    .destinationKey(target.key());

            if (metadataOverride != null) {
                builder.metadataDirective(MetadataDirective.REPLACE);
                applyMetadata(builder, metadataOverride);
            } else {
                builder.metadataDirective(MetadataDirective.COPY);
            }

            CopyObjectResponse response = client.copyObject(builder.build());
            String eTag = response.copyObjectResult() == null ? null : response.copyObjectResult().eTag();
            return new ObjectWriteResult(target, eTag, response.versionId());
        } catch (SdkException e) {
            throw objectFailure("copy", source, e);
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
        return super.unwrap(type);
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

    private static void applyMetadata(PutObjectRequest.Builder builder, ObjectMetadata metadata) {
        ObjectMetadataMapper.apply(
                metadata,
                builder::contentType,
                ignored -> { },
                builder::cacheControl,
                builder::contentDisposition,
                builder::metadata
        );
    }

    private static void applyMetadata(CopyObjectRequest.Builder builder, ObjectMetadata metadata) {
        ObjectMetadataMapper.apply(
                metadata,
                builder::contentType,
                ignored -> { },
                builder::cacheControl,
                builder::contentDisposition,
                builder::metadata
        );
    }
}
