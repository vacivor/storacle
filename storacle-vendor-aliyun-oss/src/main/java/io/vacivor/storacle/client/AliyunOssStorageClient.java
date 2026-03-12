package io.vacivor.storacle.client;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.OSSObject;
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
import java.util.List;

public final class AliyunOssStorageClient extends AbstractObjectStorageClient {
    private final OSS client;

    public AliyunOssStorageClient(OSS client) {
        this.client = java.util.Objects.requireNonNull(client, "client must not be null");
    }

    @Override
    public ObjectWriteResult put(ObjectPath path, InputStream content, ObjectMetadata metadata) {
        path = requirePath(path);
        content = requireContent(content);
        ObjectMetadata safeMetadata = safeMetadata(metadata);

        try {
            com.aliyun.oss.model.ObjectMetadata ossMetadata = new com.aliyun.oss.model.ObjectMetadata();
            ObjectMetadataMapper.apply(
                    safeMetadata,
                    ossMetadata::setContentType,
                    ossMetadata::setContentLength,
                    ossMetadata::setCacheControl,
                    ossMetadata::setContentDisposition,
                    ossMetadata::setUserMetadata
            );

            client.putObject(path.bucket(), path.key(), content, ossMetadata);
            return new ObjectWriteResult(path, null, null);
        } catch (Exception e) {
            throw objectFailure("put", path, e);
        }
    }

    @Override
    public StorageObject get(ObjectPath path) {
        path = requirePath(path);
        try {
            OSSObject object = client.getObject(path.bucket(), path.key());
            com.aliyun.oss.model.ObjectMetadata meta = object.getObjectMetadata();
            ObjectMetadata metadata = ObjectMetadataMapper.from(
                    meta::getContentType,
                    meta::getContentLength,
                    meta::getCacheControl,
                    meta::getContentDisposition,
                    meta::getUserMetadata
            );
            return ObjectStorageValueFactory.storageObject(path, metadata, object.getObjectContent());
        } catch (Exception e) {
            throw objectFailure("get", path, e);
        }
    }

    @Override
    public boolean delete(ObjectPath path) {
        path = requirePath(path);
        try {
            client.deleteObject(path.bucket(), path.key());
            return true;
        } catch (Exception e) {
            throw objectFailure("delete", path, e);
        }
    }

    @Override
    public boolean exists(ObjectPath path) {
        path = requirePath(path);
        try {
            return client.doesObjectExist(path.bucket(), path.key());
        } catch (Exception e) {
            throw objectFailure("check object existence", path, e);
        }
    }

    @Override
    public ListObjectsPage list(ListObjectsRequest request) {
        ListObjectsRequest validatedRequest = requireListRequest(request);
        try {
            com.aliyun.oss.model.ListObjectsRequest ossRequest = new com.aliyun.oss.model.ListObjectsRequest(validatedRequest.bucket())
                    .withPrefix(validatedRequest.prefix())
                    .withMaxKeys(validatedRequest.maxKeys())
                    .withMarker(validatedRequest.continuationToken());
            ObjectListing listing = client.listObjects(ossRequest);
            List<ObjectSummary> summaries = listing.getObjectSummaries().stream()
                    .map(summary -> ObjectStorageValueFactory.objectSummary(
                            validatedRequest.bucket(),
                            summary.getKey(),
                            summary.getSize(),
                            summary.getLastModified() == null ? null : summary.getLastModified().toInstant(),
                            summary.getETag()))
                    .toList();
            return new ListObjectsPage(summaries, listing.getNextMarker(), listing.isTruncated());
        } catch (Exception e) {
            throw bucketFailure("list", validatedRequest.bucket(), e);
        }
    }

    @Override
    public ObjectWriteResult copy(ObjectPath source, ObjectPath target, ObjectMetadata metadataOverride) {
        source = requirePath(source);
        target = requirePath(target);
        try {
            if (metadataOverride == null) {
                CopyObjectResult result = client.copyObject(source.bucket(), source.key(), target.bucket(), target.key());
                return new ObjectWriteResult(target, result.getETag(), null);
            }

            com.aliyun.oss.model.ObjectMetadata meta = new com.aliyun.oss.model.ObjectMetadata();
            ObjectMetadataMapper.apply(
                    metadataOverride,
                    meta::setContentType,
                    ignored -> { },
                    meta::setCacheControl,
                    meta::setContentDisposition,
                    meta::setUserMetadata
            );

            CopyObjectRequest request = new CopyObjectRequest(source.bucket(), source.key(), target.bucket(), target.key());
            request.setNewObjectMetadata(meta);
            CopyObjectResult result = client.copyObject(request);
            return new ObjectWriteResult(target, result.getETag(), null);
        } catch (Exception e) {
            throw objectFailure("copy", source, e);
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
        return super.unwrap(type);
    }
}
