package io.vacivor.storacle.client;

import java.util.Objects;

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
import io.vacivor.storacle.ObjectPath;
import io.vacivor.storacle.ObjectStorageClient;
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

public final class MinioStorageClient implements ObjectStorageClient {
    private static final long DEFAULT_PART_SIZE = 10 * 1024 * 1024;
    private final MinioClient client;

    public MinioStorageClient(MinioClient client) {
        this.client = Objects.requireNonNull(client, "client must not be null");
    }

    @Override
    public ObjectWriteResult put(ObjectPath path, InputStream content, ObjectMetadata metadata) {
        Objects.requireNonNull(path, "path must not be null");
        Objects.requireNonNull(content, "content must not be null");
        ObjectMetadata safeMetadata = metadata == null ? ObjectMetadata.empty() : metadata;

        try {
            PutObjectArgs.Builder builder = PutObjectArgs.builder()
                    .bucket(path.bucket())
                    .object(path.key());

            long size = safeMetadata.contentLength() == null ? -1 : safeMetadata.contentLength();
            if (size < 0) {
                builder.stream(content, -1, DEFAULT_PART_SIZE);
            } else {
                builder.stream(content, size, -1);
            }

            if (safeMetadata.contentType() != null) {
                builder.contentType(safeMetadata.contentType());
            }

            Map<String, String> headers = new HashMap<>();
            if (safeMetadata.cacheControl() != null) {
                headers.put("Cache-Control", safeMetadata.cacheControl());
            }
            if (safeMetadata.contentDisposition() != null) {
                headers.put("Content-Disposition", safeMetadata.contentDisposition());
            }
            if (!headers.isEmpty()) {
                builder.headers(headers);
            }

            if (!safeMetadata.userMetadata().isEmpty()) {
                builder.userMetadata(safeMetadata.userMetadata());
            }

            io.minio.ObjectWriteResponse response = client.putObject(builder.build());
            return new ObjectWriteResult(path, response.etag(), null);
        } catch (Exception e) {
            throw new StorageException("Failed to put object: " + path.bucket() + "/" + path.key(), e);
        }
    }

    @Override
    public StorageObject get(ObjectPath path) {
        Objects.requireNonNull(path, "path must not be null");
        try {
            StatObjectResponse stat = client.statObject(StatObjectArgs.builder()
                    .bucket(path.bucket())
                    .object(path.key())
                    .build());
            ObjectMetadata metadata = ObjectMetadata.builder()
                    .contentType(stat.contentType())
                    .contentLength(stat.size())
                    .userMetadata(stat.userMetadata())
                    .build();

            InputStream stream = client.getObject(GetObjectArgs.builder()
                    .bucket(path.bucket())
                    .object(path.key())
                    .build());
            return new StorageObject(path, metadata, stream);
        } catch (Exception e) {
            throw new StorageException("Failed to get object: " + path.bucket() + "/" + path.key(), e);
        }
    }

    @Override
    public boolean delete(ObjectPath path) {
        Objects.requireNonNull(path, "path must not be null");
        try {
            client.removeObject(RemoveObjectArgs.builder()
                    .bucket(path.bucket())
                    .object(path.key())
                    .build());
            return true;
        } catch (Exception e) {
            throw new StorageException("Failed to delete object: " + path.bucket() + "/" + path.key(), e);
        }
    }

    @Override
    public boolean exists(ObjectPath path) {
        Objects.requireNonNull(path, "path must not be null");
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
            throw new StorageException("Failed to check object existence: " + path.bucket() + "/" + path.key(), e);
        } catch (MinioException e) {
            throw new StorageException("Failed to check object existence: " + path.bucket() + "/" + path.key(), e);
        } catch (Exception e) {
            throw new StorageException("Failed to check object existence: " + path.bucket() + "/" + path.key(), e);
        }
    }

    @Override
    public ListObjectsPage list(ListObjectsRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        try {
            ListObjectsArgs.Builder builder = ListObjectsArgs.builder()
                    .bucket(request.bucket())
                    .prefix(request.prefix())
                    .maxKeys(request.maxKeys())
                    .continuationToken(request.continuationToken())
                    .recursive(true);

            Iterable<Result<Item>> results = client.listObjects(builder.build());
            List<ObjectSummary> summaries = new ArrayList<>();
            int count = 0;
            boolean truncated = false;
            for (Result<Item> result : results) {
                Item item = result.get();
                summaries.add(new ObjectSummary(
                        ObjectPath.of(request.bucket(), item.objectName()),
                        item.size(),
                        item.lastModified() == null ? null : Instant.from(item.lastModified()),
                        item.etag()));
                count++;
                if (count >= request.maxKeys()) {
                    truncated = true;
                    break;
                }
            }
            return new ListObjectsPage(summaries, null, truncated);
        } catch (Exception e) {
            throw new StorageException("Failed to list objects for bucket: " + request.bucket(), e);
        }
    }

    @Override
    public ObjectWriteResult copy(ObjectPath source, ObjectPath target, ObjectMetadata metadataOverride) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(target, "target must not be null");
        try {
            CopySource copySource = CopySource.builder()
                    .bucket(source.bucket())
                    .object(source.key())
                    .build();

            CopyObjectArgs.Builder builder = CopyObjectArgs.builder()
                    .bucket(target.bucket())
                    .object(target.key())
                    .source(copySource);

            if (metadataOverride != null) {
                Map<String, String> headers = new HashMap<>();
                if (metadataOverride.cacheControl() != null) {
                    headers.put("Cache-Control", metadataOverride.cacheControl());
                }
                if (metadataOverride.contentDisposition() != null) {
                    headers.put("Content-Disposition", metadataOverride.contentDisposition());
                }
                if (metadataOverride.contentType() != null) {
                    headers.put("Content-Type", metadataOverride.contentType());
                }
                if (!headers.isEmpty()) {
                    builder.headers(headers);
                }
                if (metadataOverride.userMetadata() != null) {
                    builder.userMetadata(metadataOverride.userMetadata());
                }
            }

            client.copyObject(builder.build());
            return new ObjectWriteResult(target, null, null);
        } catch (Exception e) {
            throw new StorageException("Failed to copy object: " + source.bucket() + "/" + source.key(), e);
        }
    }

    @Override
    public <T> T unwrap(Class<T> type) {
        if (type.isInstance(client)) {
            return type.cast(client);
        }
        return ObjectStorageClient.super.unwrap(type);
    }

    @Override
    public void close() {
        // MinioClient does not require explicit close.
    }
}
