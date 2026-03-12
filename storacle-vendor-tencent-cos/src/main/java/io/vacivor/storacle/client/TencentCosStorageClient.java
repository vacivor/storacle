package io.vacivor.storacle.client;

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

public final class TencentCosStorageClient extends AbstractObjectStorageClient {
    private final COSClient client;

    public TencentCosStorageClient(COSClient client) {
        this.client = java.util.Objects.requireNonNull(client, "client must not be null");
    }

    @Override
    public ObjectWriteResult put(ObjectPath path, InputStream content, ObjectMetadata metadata) {
        path = requirePath(path);
        content = requireContent(content);
        ObjectMetadata safeMetadata = safeMetadata(metadata);
        try {
            com.qcloud.cos.model.ObjectMetadata cosMetadata = new com.qcloud.cos.model.ObjectMetadata();
            ObjectMetadataMapper.apply(
                    safeMetadata,
                    cosMetadata::setContentType,
                    cosMetadata::setContentLength,
                    cosMetadata::setCacheControl,
                    cosMetadata::setContentDisposition,
                    cosMetadata::setUserMetadata
            );

            PutObjectRequest request = new PutObjectRequest(path.bucket(), path.key(), content, cosMetadata);
            PutObjectResult result = client.putObject(request);
            return new ObjectWriteResult(path, result.getETag(), null);
        } catch (CosServiceException e) {
            throw objectFailure("put", path, e);
        }
    }

    @Override
    public StorageObject get(ObjectPath path) {
        path = requirePath(path);
        try {
            COSObject obj = client.getObject(new GetObjectRequest(path.bucket(), path.key()));
            com.qcloud.cos.model.ObjectMetadata cosMeta = obj.getObjectMetadata();
            ObjectMetadata metadata = ObjectMetadataMapper.from(
                    cosMeta::getContentType,
                    cosMeta::getContentLength,
                    cosMeta::getCacheControl,
                    cosMeta::getContentDisposition,
                    cosMeta::getUserMetadata
            );
            return ObjectStorageValueFactory.storageObject(path, metadata, obj.getObjectContent());
        } catch (CosServiceException e) {
            throw objectFailure("get", path, e);
        }
    }

    @Override
    public boolean delete(ObjectPath path) {
        path = requirePath(path);
        try {
            client.deleteObject(new DeleteObjectRequest(path.bucket(), path.key()));
            return true;
        } catch (CosServiceException e) {
            throw objectFailure("delete", path, e);
        }
    }

    @Override
    public boolean exists(ObjectPath path) {
        path = requirePath(path);
        try {
            return client.doesObjectExist(path.bucket(), path.key());
        } catch (CosServiceException e) {
            throw objectFailure("check object existence", path, e);
        }
    }

    @Override
    public ListObjectsPage list(ListObjectsRequest request) {
        ListObjectsRequest validatedRequest = requireListRequest(request);
        try {
            com.qcloud.cos.model.ListObjectsRequest cosRequest = new com.qcloud.cos.model.ListObjectsRequest();
            cosRequest.setBucketName(validatedRequest.bucket());
            cosRequest.setPrefix(validatedRequest.prefix());
            cosRequest.setMaxKeys(validatedRequest.maxKeys());
            cosRequest.setMarker(validatedRequest.continuationToken());

            ObjectListing listing = client.listObjects(cosRequest);
            List<ObjectSummary> summaries = listing.getObjectSummaries().stream()
                    .map(summary -> ObjectStorageValueFactory.objectSummary(
                            validatedRequest.bucket(),
                            summary.getKey(),
                            summary.getSize(),
                            summary.getLastModified() == null ? null : summary.getLastModified().toInstant(),
                            summary.getETag()))
                    .toList();

            return new ListObjectsPage(summaries, listing.getNextMarker(), listing.isTruncated());
        } catch (CosServiceException e) {
            throw bucketFailure("list", validatedRequest.bucket(), e);
        }
    }

    @Override
    public ObjectWriteResult copy(ObjectPath source, ObjectPath target, ObjectMetadata metadataOverride) {
        source = requirePath(source);
        target = requirePath(target);
        try {
            CopyObjectRequest copyRequest = new CopyObjectRequest(source.bucket(), source.key(),
                    target.bucket(), target.key());
            if (metadataOverride != null) {
                com.qcloud.cos.model.ObjectMetadata meta = new com.qcloud.cos.model.ObjectMetadata();
                ObjectMetadataMapper.apply(
                        metadataOverride,
                        meta::setContentType,
                        ignored -> { },
                        meta::setCacheControl,
                        meta::setContentDisposition,
                        meta::setUserMetadata
                );
                copyRequest.setNewObjectMetadata(meta);
            }
            CopyObjectResult result = client.copyObject(copyRequest);
            return new ObjectWriteResult(target, result.getETag(), null);
        } catch (CosServiceException e) {
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
        client.shutdown();
    }
}
