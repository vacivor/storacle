package io.vacivor.storacle.client;

import java.util.Objects;

import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.CopyRequest;
import com.google.cloud.storage.Storage.BlobListOption;
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
import java.nio.channels.Channels;
import java.time.Instant;

public final class GoogleCloudStorageClient implements ObjectStorageClient {
    private final Storage storage;

    public GoogleCloudStorageClient(Storage storage) {
        this.storage = Objects.requireNonNull(storage, "storage must not be null");
    }

    @Override
    public ObjectWriteResult put(ObjectPath path, InputStream content, ObjectMetadata metadata) {
        Objects.requireNonNull(path, "path must not be null");
        Objects.requireNonNull(content, "content must not be null");
        ObjectMetadata safeMetadata = metadata == null ? ObjectMetadata.empty() : metadata;

        try {
            BlobId blobId = BlobId.of(path.bucket(), path.key());
            BlobInfo.Builder infoBuilder = BlobInfo.newBuilder(blobId);
            if (safeMetadata.contentType() != null) {
                infoBuilder.setContentType(safeMetadata.contentType());
            }
            if (safeMetadata.cacheControl() != null) {
                infoBuilder.setCacheControl(safeMetadata.cacheControl());
            }
            if (safeMetadata.contentDisposition() != null) {
                infoBuilder.setContentDisposition(safeMetadata.contentDisposition());
            }
            if (!safeMetadata.userMetadata().isEmpty()) {
                infoBuilder.setMetadata(safeMetadata.userMetadata());
            }

            storage.create(infoBuilder.build(), content);
            return new ObjectWriteResult(path, null, null);
        } catch (Exception e) {
            throw new StorageException("Failed to put object: " + path.bucket() + "/" + path.key(), e);
        }
    }

    @Override
    public StorageObject get(ObjectPath path) {
        Objects.requireNonNull(path, "path must not be null");
        try {
            BlobId blobId = BlobId.of(path.bucket(), path.key());
            Blob blob = storage.get(blobId);
            if (blob == null) {
                throw new StorageException("Object not found: " + path.bucket() + "/" + path.key());
            }

            ObjectMetadata metadata = ObjectMetadata.builder()
                    .contentType(blob.getContentType())
                    .contentLength(blob.getSize())
                    .cacheControl(blob.getCacheControl())
                    .contentDisposition(blob.getContentDisposition())
                    .userMetadata(blob.getMetadata())
                    .build();

            ReadChannel channel = storage.reader(blobId);
            return new StorageObject(path, metadata, Channels.newInputStream(channel));
        } catch (Exception e) {
            throw new StorageException("Failed to get object: " + path.bucket() + "/" + path.key(), e);
        }
    }

    @Override
    public boolean delete(ObjectPath path) {
        Objects.requireNonNull(path, "path must not be null");
        try {
            return storage.delete(BlobId.of(path.bucket(), path.key()));
        } catch (Exception e) {
            throw new StorageException("Failed to delete object: " + path.bucket() + "/" + path.key(), e);
        }
    }

    @Override
    public boolean exists(ObjectPath path) {
        Objects.requireNonNull(path, "path must not be null");
        try {
            return storage.get(BlobId.of(path.bucket(), path.key())) != null;
        } catch (Exception e) {
            throw new StorageException("Failed to check object existence: " + path.bucket() + "/" + path.key(), e);
        }
    }

    @Override
    public ListObjectsPage list(ListObjectsRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        try {
            if (request.continuationToken() == null || request.continuationToken().isBlank()) {
                Page<Blob> page = storage.list(
                    request.bucket(),
                    BlobListOption.prefix(request.prefix()),
                    BlobListOption.pageSize(request.maxKeys())
                );
                return toPage(request.bucket(), page);
            }
            Page<Blob> page = storage.list(
                    request.bucket(),
                    BlobListOption.prefix(request.prefix()),
                    BlobListOption.pageSize(request.maxKeys()),
                    BlobListOption.pageToken(request.continuationToken())
            );

            return toPage(request.bucket(), page);
        } catch (Exception e) {
            throw new StorageException("Failed to list objects for bucket: " + request.bucket(), e);
        }
    }

    @Override
    public ObjectWriteResult copy(ObjectPath source, ObjectPath target, ObjectMetadata metadataOverride) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(target, "target must not be null");
        try {
            BlobId sourceId = BlobId.of(source.bucket(), source.key());
            BlobId targetId = BlobId.of(target.bucket(), target.key());

            CopyRequest.Builder builder = CopyRequest.newBuilder().setSource(sourceId).setTarget(targetId);
            if (metadataOverride != null) {
                BlobInfo.Builder infoBuilder = BlobInfo.newBuilder(targetId);
                if (metadataOverride.contentType() != null) {
                    infoBuilder.setContentType(metadataOverride.contentType());
                }
                if (metadataOverride.cacheControl() != null) {
                    infoBuilder.setCacheControl(metadataOverride.cacheControl());
                }
                if (metadataOverride.contentDisposition() != null) {
                    infoBuilder.setContentDisposition(metadataOverride.contentDisposition());
                }
                if (!metadataOverride.userMetadata().isEmpty()) {
                    infoBuilder.setMetadata(metadataOverride.userMetadata());
                }
                builder.setTarget(infoBuilder.build());
            }

            CopyWriter writer = storage.copy(builder.build());
            Blob copied = writer.getResult();
            return new ObjectWriteResult(target, copied.getEtag(), null);
        } catch (Exception e) {
            throw new StorageException("Failed to copy object: " + source.bucket() + "/" + source.key(), e);
        }
    }

    @Override
    public void close() {
        // Storage client is managed by the SDK; no explicit close required.
    }

    @Override
    public <T> T unwrap(Class<T> type) {
        if (type.isInstance(storage)) {
            return type.cast(storage);
        }
        return ObjectStorageClient.super.unwrap(type);
    }

    private static ListObjectsPage toPage(String bucket, Page<Blob> page) {
        java.util.List<ObjectSummary> summaries = new java.util.ArrayList<>();
        for (Blob blob : page.getValues()) {
            Instant updatedAt = blob.getUpdateTime() == null ? null : Instant.ofEpochMilli(blob.getUpdateTime());
            summaries.add(new ObjectSummary(
                    ObjectPath.of(bucket, blob.getName()),
                    blob.getSize(),
                    updatedAt,
                    blob.getEtag()));
        }

        String nextToken = page.getNextPageToken();
        boolean truncated = page.hasNextPage();
        return new ListObjectsPage(summaries, nextToken, truncated);
    }
}
