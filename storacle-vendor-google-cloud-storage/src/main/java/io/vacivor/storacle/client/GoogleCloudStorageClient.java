package io.vacivor.storacle.client;

import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.CopyRequest;
import com.google.cloud.storage.Storage.BlobListOption;
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

import java.io.InputStream;
import java.nio.channels.Channels;
import java.time.Instant;

public final class GoogleCloudStorageClient extends AbstractObjectStorageClient {
    private final Storage storage;

    public GoogleCloudStorageClient(Storage storage) {
        this.storage = java.util.Objects.requireNonNull(storage, "storage must not be null");
    }

    @Override
    public ObjectWriteResult put(ObjectPath path, InputStream content, ObjectMetadata metadata) {
        path = requirePath(path);
        content = requireContent(content);
        ObjectMetadata safeMetadata = safeMetadata(metadata);

        try {
            BlobId blobId = BlobId.of(path.bucket(), path.key());
            BlobInfo.Builder infoBuilder = BlobInfo.newBuilder(blobId);
            applyMetadata(infoBuilder, safeMetadata);

            storage.create(infoBuilder.build(), content);
            return new ObjectWriteResult(path, null, null);
        } catch (Exception e) {
            throw objectFailure("put", path, e);
        }
    }

    @Override
    public StorageObject get(ObjectPath path) {
        path = requirePath(path);
        try {
            BlobId blobId = BlobId.of(path.bucket(), path.key());
            Blob blob = storage.get(blobId);
            if (blob == null) {
                throw new StorageException("Object not found: " + path.bucket() + "/" + path.key());
            }

            ObjectMetadata metadata = ObjectMetadataMapper.from(
                    blob::getContentType,
                    blob::getSize,
                    blob::getCacheControl,
                    blob::getContentDisposition,
                    blob::getMetadata
            );

            ReadChannel channel = storage.reader(blobId);
            return ObjectStorageValueFactory.storageObject(path, metadata, Channels.newInputStream(channel));
        } catch (Exception e) {
            throw objectFailure("get", path, e);
        }
    }

    @Override
    public boolean delete(ObjectPath path) {
        path = requirePath(path);
        try {
            return storage.delete(BlobId.of(path.bucket(), path.key()));
        } catch (Exception e) {
            throw objectFailure("delete", path, e);
        }
    }

    @Override
    public boolean exists(ObjectPath path) {
        path = requirePath(path);
        try {
            return storage.get(BlobId.of(path.bucket(), path.key())) != null;
        } catch (Exception e) {
            throw objectFailure("check object existence", path, e);
        }
    }

    @Override
    public ListObjectsPage list(ListObjectsRequest request) {
        ListObjectsRequest validatedRequest = requireListRequest(request);
        try {
            if (validatedRequest.continuationToken() == null || validatedRequest.continuationToken().isBlank()) {
                Page<Blob> page = storage.list(
                    validatedRequest.bucket(),
                    BlobListOption.prefix(validatedRequest.prefix()),
                    BlobListOption.pageSize(validatedRequest.maxKeys())
                );
                return toPage(validatedRequest.bucket(), page);
            }
            Page<Blob> page = storage.list(
                    validatedRequest.bucket(),
                    BlobListOption.prefix(validatedRequest.prefix()),
                    BlobListOption.pageSize(validatedRequest.maxKeys()),
                    BlobListOption.pageToken(validatedRequest.continuationToken())
            );

            return toPage(validatedRequest.bucket(), page);
        } catch (Exception e) {
            throw bucketFailure("list", validatedRequest.bucket(), e);
        }
    }

    @Override
    public ObjectWriteResult copy(ObjectPath source, ObjectPath target, ObjectMetadata metadataOverride) {
        source = requirePath(source);
        target = requirePath(target);
        try {
            BlobId sourceId = BlobId.of(source.bucket(), source.key());
            BlobId targetId = BlobId.of(target.bucket(), target.key());

            CopyRequest.Builder builder = CopyRequest.newBuilder().setSource(sourceId).setTarget(targetId);
            if (metadataOverride != null) {
                BlobInfo.Builder infoBuilder = BlobInfo.newBuilder(targetId);
                applyMetadata(infoBuilder, metadataOverride);
                builder.setTarget(infoBuilder.build());
            }

            CopyWriter writer = storage.copy(builder.build());
            Blob copied = writer.getResult();
            return new ObjectWriteResult(target, copied.getEtag(), null);
        } catch (Exception e) {
            throw objectFailure("copy", source, e);
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
        return super.unwrap(type);
    }

    private static ListObjectsPage toPage(String bucket, Page<Blob> page) {
        java.util.List<ObjectSummary> summaries = new java.util.ArrayList<>();
        for (Blob blob : page.getValues()) {
            Instant updatedAt = blob.getUpdateTime() == null ? null : Instant.ofEpochMilli(blob.getUpdateTime());
            summaries.add(ObjectStorageValueFactory.objectSummary(
                    bucket,
                    blob.getName(),
                    blob.getSize(),
                    updatedAt,
                    blob.getEtag()));
        }

        String nextToken = page.getNextPageToken();
        boolean truncated = page.hasNextPage();
        return new ListObjectsPage(summaries, nextToken, truncated);
    }

    private static void applyMetadata(BlobInfo.Builder builder, ObjectMetadata metadata) {
        ObjectMetadataMapper.apply(
                metadata,
                builder::setContentType,
                ignored -> { },
                builder::setCacheControl,
                builder::setContentDisposition,
                builder::setMetadata
        );
    }
}
