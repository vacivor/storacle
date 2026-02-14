package io.vacivor.storacle.client;

import java.util.Objects;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.Region;
import com.qiniu.storage.UploadManager;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.Auth;
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
import java.net.URL;
import java.time.Instant;
import java.util.List;

public final class QiniuKodoStorageClient implements ObjectStorageClient {
    private final Auth auth;
    private final UploadManager uploadManager;
    private final BucketManager bucketManager;
    private final QiniuDownloadUrlProvider downloadUrlProvider;

    public QiniuKodoStorageClient(Auth auth, UploadManager uploadManager, BucketManager bucketManager,
                                 QiniuDownloadUrlProvider downloadUrlProvider) {
        this.auth = Objects.requireNonNull(auth, "auth must not be null");
        this.uploadManager = Objects.requireNonNull(uploadManager, "uploadManager must not be null");
        this.bucketManager = Objects.requireNonNull(bucketManager, "bucketManager must not be null");
        this.downloadUrlProvider = Objects.requireNonNull(downloadUrlProvider, "downloadUrlProvider must not be null");
    }

    @Override
    public ObjectWriteResult put(ObjectPath path, InputStream content, ObjectMetadata metadata) {
        Objects.requireNonNull(path, "path must not be null");
        Objects.requireNonNull(content, "content must not be null");
        ObjectMetadata safeMetadata = metadata == null ? ObjectMetadata.empty() : metadata;

        try {
            String token = auth.uploadToken(path.bucket());
            String mime = safeMetadata.contentType();
            Response response = uploadManager.put(content, path.key(), token, null, mime);
            return new ObjectWriteResult(path, response.isOK() ? response.reqId : null, null);
        } catch (QiniuException e) {
            throw new StorageException("Failed to put object: " + path.bucket() + "/" + path.key(), e);
        }
    }

    @Override
    public StorageObject get(ObjectPath path) {
        Objects.requireNonNull(path, "path must not be null");
        try {
            FileInfo info = bucketManager.stat(path.bucket(), path.key());
            ObjectMetadata metadata = ObjectMetadata.builder()
                    .contentType(info.mimeType)
                    .contentLength(info.fsize)
                    .build();

            String url = downloadUrlProvider.resolve(path.bucket(), path.key());

            InputStream stream = new URL(url).openStream();
            return new StorageObject(path, metadata, stream);
        } catch (QiniuException e) {
            throw new StorageException("Failed to get object: " + path.bucket() + "/" + path.key(), e);
        } catch (Exception e) {
            throw new StorageException("Failed to get object: " + path.bucket() + "/" + path.key(), e);
        }
    }

    @Override
    public boolean delete(ObjectPath path) {
        Objects.requireNonNull(path, "path must not be null");
        try {
            bucketManager.delete(path.bucket(), path.key());
            return true;
        } catch (QiniuException e) {
            throw new StorageException("Failed to delete object: " + path.bucket() + "/" + path.key(), e);
        }
    }

    @Override
    public boolean exists(ObjectPath path) {
        Objects.requireNonNull(path, "path must not be null");
        try {
            bucketManager.stat(path.bucket(), path.key());
            return true;
        } catch (QiniuException e) {
            if (e.code() == 612) {
                return false;
            }
            throw new StorageException("Failed to check object existence: " + path.bucket() + "/" + path.key(), e);
        }
    }

    @Override
    public ListObjectsPage list(ListObjectsRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        try {
            FileListing listing = bucketManager.listFiles(request.bucket(), request.prefix(),
                    request.continuationToken(), request.maxKeys(), null);
            List<ObjectSummary> summaries = List.of();
            if (listing.items != null) {
                summaries = java.util.Arrays.stream(listing.items)
                        .map(item -> new ObjectSummary(
                                ObjectPath.of(request.bucket(), item.key),
                                item.fsize,
                                item.putTime == 0 ? null : Instant.ofEpochMilli(item.putTime / 10000),
                                item.hash))
                        .toList();
            }
            boolean truncated = listing.marker != null && !listing.marker.isBlank();
            return new ListObjectsPage(summaries, listing.marker, truncated);
        } catch (QiniuException e) {
            throw new StorageException("Failed to list objects for bucket: " + request.bucket(), e);
        }
    }

    @Override
    public ObjectWriteResult copy(ObjectPath source, ObjectPath target, ObjectMetadata metadataOverride) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(target, "target must not be null");
        try {
            bucketManager.copy(source.bucket(), source.key(), target.bucket(), target.key());
            return new ObjectWriteResult(target, null, null);
        } catch (QiniuException e) {
            throw new StorageException("Failed to copy object: " + source.bucket() + "/" + source.key(), e);
        }
    }

    @Override
    public void close() {
        // Qiniu clients are stateless.
    }

    @Override
    public <T> T unwrap(Class<T> type) {
        if (type.isInstance(auth)) {
            return type.cast(auth);
        }
        if (type.isInstance(uploadManager)) {
            return type.cast(uploadManager);
        }
        if (type.isInstance(bucketManager)) {
            return type.cast(bucketManager);
        }
        if (type.isInstance(downloadUrlProvider)) {
            return type.cast(downloadUrlProvider);
        }
        return ObjectStorageClient.super.unwrap(type);
    }

    public static Configuration resolveConfiguration(String regionCode) {
        Region region = Region.autoRegion();
        if (regionCode != null) {
            region = switch (regionCode) {
                case "z0" -> Region.region0();
                case "z1" -> Region.region1();
                case "z2" -> Region.region2();
                case "na0" -> Region.regionNa0();
                case "as0" -> Region.regionAs0();
                default -> Region.autoRegion();
            };
        }
        return new Configuration(region);
    }
}
