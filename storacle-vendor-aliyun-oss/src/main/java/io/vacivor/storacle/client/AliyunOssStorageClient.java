package io.vacivor.storacle.client;

import java.util.Objects;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.OSSObject;
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
import java.util.List;

public final class AliyunOssStorageClient implements ObjectStorageClient {
    private final OSS client;

    public AliyunOssStorageClient(OSS client) {
        this.client = Objects.requireNonNull(client, "client must not be null");
    }

    @Override
    public ObjectWriteResult put(ObjectPath path, InputStream content, ObjectMetadata metadata) {
        Objects.requireNonNull(path, "path must not be null");
        Objects.requireNonNull(content, "content must not be null");
        ObjectMetadata safeMetadata = metadata == null ? ObjectMetadata.empty() : metadata;

        try {
            com.aliyun.oss.model.ObjectMetadata ossMetadata = new com.aliyun.oss.model.ObjectMetadata();
            if (safeMetadata.contentType() != null) {
                ossMetadata.setContentType(safeMetadata.contentType());
            }
            if (safeMetadata.contentLength() != null && safeMetadata.contentLength() >= 0) {
                ossMetadata.setContentLength(safeMetadata.contentLength());
            }
            if (safeMetadata.cacheControl() != null) {
                ossMetadata.setCacheControl(safeMetadata.cacheControl());
            }
            if (safeMetadata.contentDisposition() != null) {
                ossMetadata.setContentDisposition(safeMetadata.contentDisposition());
            }
            if (!safeMetadata.userMetadata().isEmpty()) {
                ossMetadata.setUserMetadata(safeMetadata.userMetadata());
            }

            client.putObject(path.bucket(), path.key(), content, ossMetadata);
            return new ObjectWriteResult(path, null, null);
        } catch (Exception e) {
            throw new StorageException("Failed to put object: " + path.bucket() + "/" + path.key(), e);
        }
    }

    @Override
    public StorageObject get(ObjectPath path) {
        Objects.requireNonNull(path, "path must not be null");
        try {
            OSSObject object = client.getObject(path.bucket(), path.key());
            com.aliyun.oss.model.ObjectMetadata meta = object.getObjectMetadata();
            ObjectMetadata metadata = ObjectMetadata.builder()
                    .contentType(meta.getContentType())
                    .contentLength(meta.getContentLength())
                    .cacheControl(meta.getCacheControl())
                    .contentDisposition(meta.getContentDisposition())
                    .userMetadata(meta.getUserMetadata())
                    .build();
            return new StorageObject(path, metadata, object.getObjectContent());
        } catch (Exception e) {
            throw new StorageException("Failed to get object: " + path.bucket() + "/" + path.key(), e);
        }
    }

    @Override
    public boolean delete(ObjectPath path) {
        Objects.requireNonNull(path, "path must not be null");
        try {
            client.deleteObject(path.bucket(), path.key());
            return true;
        } catch (Exception e) {
            throw new StorageException("Failed to delete object: " + path.bucket() + "/" + path.key(), e);
        }
    }

    @Override
    public boolean exists(ObjectPath path) {
        Objects.requireNonNull(path, "path must not be null");
        try {
            return client.doesObjectExist(path.bucket(), path.key());
        } catch (Exception e) {
            throw new StorageException("Failed to check object existence: " + path.bucket() + "/" + path.key(), e);
        }
    }

    @Override
    public ListObjectsPage list(ListObjectsRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        try {
            com.aliyun.oss.model.ListObjectsRequest ossRequest = new com.aliyun.oss.model.ListObjectsRequest(request.bucket())
                    .withPrefix(request.prefix())
                    .withMaxKeys(request.maxKeys())
                    .withMarker(request.continuationToken());
            ObjectListing listing = client.listObjects(ossRequest);
            List<ObjectSummary> summaries = listing.getObjectSummaries().stream()
                    .map(summary -> new ObjectSummary(
                            ObjectPath.of(request.bucket(), summary.getKey()),
                            summary.getSize(),
                            summary.getLastModified() == null ? null : summary.getLastModified().toInstant(),
                            summary.getETag()))
                    .toList();
            return new ListObjectsPage(summaries, listing.getNextMarker(), listing.isTruncated());
        } catch (Exception e) {
            throw new StorageException("Failed to list objects for bucket: " + request.bucket(), e);
        }
    }

    @Override
    public ObjectWriteResult copy(ObjectPath source, ObjectPath target, ObjectMetadata metadataOverride) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(target, "target must not be null");
        try {
            if (metadataOverride == null) {
                CopyObjectResult result = client.copyObject(source.bucket(), source.key(), target.bucket(), target.key());
                return new ObjectWriteResult(target, result.getETag(), null);
            }

            com.aliyun.oss.model.ObjectMetadata meta = new com.aliyun.oss.model.ObjectMetadata();
            if (metadataOverride.contentType() != null) {
                meta.setContentType(metadataOverride.contentType());
            }
            if (metadataOverride.cacheControl() != null) {
                meta.setCacheControl(metadataOverride.cacheControl());
            }
            if (metadataOverride.contentDisposition() != null) {
                meta.setContentDisposition(metadataOverride.contentDisposition());
            }
            if (!metadataOverride.userMetadata().isEmpty()) {
                meta.setUserMetadata(metadataOverride.userMetadata());
            }

            CopyObjectRequest request = new CopyObjectRequest(source.bucket(), source.key(), target.bucket(), target.key());
            request.setNewObjectMetadata(meta);
            CopyObjectResult result = client.copyObject(request);
            return new ObjectWriteResult(target, result.getETag(), null);
        } catch (Exception e) {
            throw new StorageException("Failed to copy object: " + source.bucket() + "/" + source.key(), e);
        }
    }

    @Override
    public void close() {
        client.shutdown();
    }

    @Override
    public <T> T unwrap(Class<T> type) {
        if (type.isInstance(client)) {
            return type.cast(client);
        }
        return ObjectStorageClient.super.unwrap(type);
    }
}
