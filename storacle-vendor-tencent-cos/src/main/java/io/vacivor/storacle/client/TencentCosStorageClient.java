package io.vacivor.storacle.client;

import java.util.Objects;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.CopyObjectRequest;
import com.qcloud.cos.model.CopyObjectResult;
import com.qcloud.cos.model.COSObject;
import com.qcloud.cos.model.DeleteObjectRequest;
import com.qcloud.cos.model.GetObjectRequest;
import com.qcloud.cos.model.ObjectListing;
import com.qcloud.cos.model.PutObjectRequest;
import com.qcloud.cos.model.PutObjectResult;
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

public final class TencentCosStorageClient implements ObjectStorageClient {
    private final COSClient client;

    public TencentCosStorageClient(COSClient client) {
        this.client = Objects.requireNonNull(client, "client must not be null");
    }

    @Override
    public ObjectWriteResult put(ObjectPath path, InputStream content, ObjectMetadata metadata) {
        Objects.requireNonNull(path, "path must not be null");
        Objects.requireNonNull(content, "content must not be null");
        ObjectMetadata safeMetadata = metadata == null ? ObjectMetadata.empty() : metadata;
        try {
            com.qcloud.cos.model.ObjectMetadata cosMetadata = new com.qcloud.cos.model.ObjectMetadata();
            if (safeMetadata.contentType() != null) {
                cosMetadata.setContentType(safeMetadata.contentType());
            }
            if (safeMetadata.contentLength() != null && safeMetadata.contentLength() >= 0) {
                cosMetadata.setContentLength(safeMetadata.contentLength());
            }
            if (safeMetadata.cacheControl() != null) {
                cosMetadata.setCacheControl(safeMetadata.cacheControl());
            }
            if (safeMetadata.contentDisposition() != null) {
                cosMetadata.setContentDisposition(safeMetadata.contentDisposition());
            }
            if (!safeMetadata.userMetadata().isEmpty()) {
                cosMetadata.setUserMetadata(safeMetadata.userMetadata());
            }

            PutObjectRequest request = new PutObjectRequest(path.bucket(), path.key(), content, cosMetadata);
            PutObjectResult result = client.putObject(request);
            return new ObjectWriteResult(path, result.getETag(), null);
        } catch (CosServiceException e) {
            throw new StorageException("Failed to put object: " + path.bucket() + "/" + path.key(), e);
        }
    }

    @Override
    public StorageObject get(ObjectPath path) {
        Objects.requireNonNull(path, "path must not be null");
        try {
            COSObject obj = client.getObject(new GetObjectRequest(path.bucket(), path.key()));
            com.qcloud.cos.model.ObjectMetadata cosMeta = obj.getObjectMetadata();
            ObjectMetadata metadata = ObjectMetadata.builder()
                    .contentType(cosMeta.getContentType())
                    .contentLength(cosMeta.getContentLength())
                    .cacheControl(cosMeta.getCacheControl())
                    .contentDisposition(cosMeta.getContentDisposition())
                    .userMetadata(cosMeta.getUserMetadata())
                    .build();
            return new StorageObject(path, metadata, obj.getObjectContent());
        } catch (CosServiceException e) {
            throw new StorageException("Failed to get object: " + path.bucket() + "/" + path.key(), e);
        }
    }

    @Override
    public boolean delete(ObjectPath path) {
        Objects.requireNonNull(path, "path must not be null");
        try {
            client.deleteObject(new DeleteObjectRequest(path.bucket(), path.key()));
            return true;
        } catch (CosServiceException e) {
            throw new StorageException("Failed to delete object: " + path.bucket() + "/" + path.key(), e);
        }
    }

    @Override
    public boolean exists(ObjectPath path) {
        Objects.requireNonNull(path, "path must not be null");
        try {
            return client.doesObjectExist(path.bucket(), path.key());
        } catch (CosServiceException e) {
            throw new StorageException("Failed to check object existence: " + path.bucket() + "/" + path.key(), e);
        }
    }

    @Override
    public ListObjectsPage list(ListObjectsRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        try {
            com.qcloud.cos.model.ListObjectsRequest cosRequest = new com.qcloud.cos.model.ListObjectsRequest();
            cosRequest.setBucketName(request.bucket());
            cosRequest.setPrefix(request.prefix());
            cosRequest.setMaxKeys(request.maxKeys());
            cosRequest.setMarker(request.continuationToken());

            ObjectListing listing = client.listObjects(cosRequest);
            List<ObjectSummary> summaries = listing.getObjectSummaries().stream()
                    .map(summary -> new ObjectSummary(
                            ObjectPath.of(request.bucket(), summary.getKey()),
                            summary.getSize(),
                            summary.getLastModified() == null ? null : summary.getLastModified().toInstant(),
                            summary.getETag()))
                    .toList();

            return new ListObjectsPage(summaries, listing.getNextMarker(), listing.isTruncated());
        } catch (CosServiceException e) {
            throw new StorageException("Failed to list objects for bucket: " + request.bucket(), e);
        }
    }

    @Override
    public ObjectWriteResult copy(ObjectPath source, ObjectPath target, ObjectMetadata metadataOverride) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(target, "target must not be null");
        try {
            CopyObjectRequest copyRequest = new CopyObjectRequest(source.bucket(), source.key(),
                    target.bucket(), target.key());
            if (metadataOverride != null) {
                com.qcloud.cos.model.ObjectMetadata meta = new com.qcloud.cos.model.ObjectMetadata();
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
                copyRequest.setNewObjectMetadata(meta);
            }
            CopyObjectResult result = client.copyObject(copyRequest);
            return new ObjectWriteResult(target, result.getETag(), null);
        } catch (CosServiceException e) {
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
        client.shutdown();
    }
}
