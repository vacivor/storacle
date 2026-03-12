package io.vacivor.storacle.client;

import io.vacivor.storacle.AbstractObjectStorageClient;
import io.minio.CopyObjectArgs;
import io.minio.CopySource;
import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.MinioException;
import io.minio.messages.Item;
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

import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class MinioStorageClient extends AbstractObjectStorageClient {
    private static final long DEFAULT_PART_SIZE = 10 * 1024 * 1024;
    private final MinioClient client;

    public MinioStorageClient(MinioClient client) {
        this.client = java.util.Objects.requireNonNull(client, "client must not be null");
    }

    @Override
    public ObjectWriteResult put(ObjectPath path, InputStream content, ObjectMetadata metadata) {
        path = requirePath(path);
        content = requireContent(content);
        ObjectMetadata safeMetadata = safeMetadata(metadata);

        try {
            io.minio.ObjectWriteResponse response = client.putObject(createPutArgs(path, content, safeMetadata));
            return new ObjectWriteResult(path, response.etag(), null);
        } catch (Exception e) {
            throw objectFailure("put", path, e);
        }
    }

    @Override
    public StorageObject get(ObjectPath path) {
        path = requirePath(path);
        try {
            StatObjectResponse stat = client.statObject(StatObjectArgs.builder()
                    .bucket(path.bucket())
                    .object(path.key())
                    .build());
            ObjectMetadata metadata = ObjectMetadataMapper.from(
                    stat::contentType,
                    stat::size,
                    () -> null,
                    () -> null,
                    stat::userMetadata
            );

            InputStream stream = client.getObject(GetObjectArgs.builder()
                    .bucket(path.bucket())
                    .object(path.key())
                    .build());
            return ObjectStorageValueFactory.storageObject(path, metadata, stream);
        } catch (Exception e) {
            throw objectFailure("get", path, e);
        }
    }

    @Override
    public boolean delete(ObjectPath path) {
        path = requirePath(path);
        try {
            client.removeObject(RemoveObjectArgs.builder()
                    .bucket(path.bucket())
                    .object(path.key())
                    .build());
            return true;
        } catch (Exception e) {
            throw objectFailure("delete", path, e);
        }
    }

    @Override
    public boolean exists(ObjectPath path) {
        path = requirePath(path);
        try {
            client.statObject(StatObjectArgs.builder()
                    .bucket(path.bucket())
                    .object(path.key())
                    .build());
            return true;
        } catch (ErrorResponseException e) {
            String code = e.errorResponse().code();
            if ("NoSuchKey".equals(code) || "NoSuchObject".equals(code) || "NoSuchBucket".equals(code)) {
                return false;
            }
            throw objectFailure("check object existence", path, e);
        } catch (MinioException e) {
            throw objectFailure("check object existence", path, e);
        } catch (Exception e) {
            throw objectFailure("check object existence", path, e);
        }
    }

    @Override
    public ListObjectsPage list(ListObjectsRequest request) {
        ListObjectsRequest validatedRequest = requireListRequest(request);
        try {
            ListObjectsArgs.Builder builder = ListObjectsArgs.builder()
                    .bucket(validatedRequest.bucket())
                    .prefix(validatedRequest.prefix())
                    .maxKeys(validatedRequest.maxKeys())
                    .continuationToken(validatedRequest.continuationToken())
                    .recursive(true);

            Iterable<Result<Item>> results = client.listObjects(builder.build());
            List<ObjectSummary> summaries = new ArrayList<>();
            int count = 0;
            boolean truncated = false;
            for (Result<Item> result : results) {
                Item item = result.get();
                summaries.add(ObjectStorageValueFactory.objectSummary(
                        validatedRequest.bucket(),
                        item.objectName(),
                        item.size(),
                        item.lastModified() == null ? null : Instant.from(item.lastModified()),
                        item.etag()));
                count++;
                if (count >= validatedRequest.maxKeys()) {
                    truncated = true;
                    break;
                }
            }
            return new ListObjectsPage(summaries, null, truncated);
        } catch (Exception e) {
            throw bucketFailure("list", validatedRequest.bucket(), e);
        }
    }

    @Override
    public ObjectWriteResult copy(ObjectPath source, ObjectPath target, ObjectMetadata metadataOverride) {
        source = requirePath(source);
        target = requirePath(target);
        try {
            client.copyObject(createCopyArgs(source, target, metadataOverride));
            return new ObjectWriteResult(target, null, null);
        } catch (Exception e) {
            throw objectFailure("copy", source, e);
        }
    }

    @Override
    public <T> T unwrap(Class<T> type) {
        if (type.isInstance(client)) {
            return type.cast(client);
        }
        return super.unwrap(type);
    }

    @Override
    public void close() {
        // MinioClient does not require explicit close.
    }

    private static PutObjectArgs createPutArgs(ObjectPath path, InputStream content, ObjectMetadata metadata) {
        PutObjectArgs.Builder builder = PutObjectArgs.builder()
                .bucket(path.bucket())
                .object(path.key());

        long size = metadata.contentLength() == null ? -1 : metadata.contentLength();
        if (size < 0) {
            builder.stream(content, -1, DEFAULT_PART_SIZE);
        } else {
            builder.stream(content, size, -1);
        }

        if (metadata.contentType() != null) {
            builder.contentType(metadata.contentType());
        }

        applyHeaders(builder, metadata, false);
        applyUserMetadata(builder, metadata);
        return builder.build();
    }

    private static CopyObjectArgs createCopyArgs(ObjectPath source, ObjectPath target, ObjectMetadata metadataOverride) {
        CopySource copySource = CopySource.builder()
                .bucket(source.bucket())
                .object(source.key())
                .build();

        CopyObjectArgs.Builder builder = CopyObjectArgs.builder()
                .bucket(target.bucket())
                .object(target.key())
                .source(copySource);

        if (metadataOverride != null) {
            applyHeaders(builder, metadataOverride, true);
            applyUserMetadata(builder, metadataOverride);
        }
        return builder.build();
    }

    private static void applyHeaders(PutObjectArgs.Builder builder, ObjectMetadata metadata, boolean includeContentType) {
        Map<String, String> headers = toHeaders(metadata, includeContentType);
        if (!headers.isEmpty()) {
            builder.headers(headers);
        }
    }

    private static void applyHeaders(CopyObjectArgs.Builder builder, ObjectMetadata metadata, boolean includeContentType) {
        Map<String, String> headers = toHeaders(metadata, includeContentType);
        if (!headers.isEmpty()) {
            builder.headers(headers);
        }
    }

    private static void applyUserMetadata(PutObjectArgs.Builder builder, ObjectMetadata metadata) {
        if (!metadata.userMetadata().isEmpty()) {
            builder.userMetadata(metadata.userMetadata());
        }
    }

    private static void applyUserMetadata(CopyObjectArgs.Builder builder, ObjectMetadata metadata) {
        if (!metadata.userMetadata().isEmpty()) {
            builder.userMetadata(metadata.userMetadata());
        }
    }

    private static Map<String, String> toHeaders(ObjectMetadata metadata, boolean includeContentType) {
        Map<String, String> headers = new HashMap<>();
        if (metadata.cacheControl() != null) {
            headers.put("Cache-Control", metadata.cacheControl());
        }
        if (metadata.contentDisposition() != null) {
            headers.put("Content-Disposition", metadata.contentDisposition());
        }
        if (includeContentType && metadata.contentType() != null) {
            headers.put("Content-Type", metadata.contentType());
        }
        return headers;
    }
}
