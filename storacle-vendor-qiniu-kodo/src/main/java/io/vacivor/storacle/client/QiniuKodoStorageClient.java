package io.vacivor.storacle.client;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.Region;
import com.qiniu.storage.UploadManager;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.Auth;
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
import java.net.URL;
import java.time.Instant;
import java.util.List;

public final class QiniuKodoStorageClient extends AbstractObjectStorageClient {
    private final Auth auth;
    private final UploadManager uploadManager;
    private final BucketManager bucketManager;
    private final QiniuDownloadUrlProvider downloadUrlProvider;

    public QiniuKodoStorageClient(Auth auth, UploadManager uploadManager, BucketManager bucketManager,
                                 QiniuDownloadUrlProvider downloadUrlProvider) {
        this.auth = java.util.Objects.requireNonNull(auth, "auth must not be null");
        this.uploadManager = java.util.Objects.requireNonNull(uploadManager, "uploadManager must not be null");
        this.bucketManager = java.util.Objects.requireNonNull(bucketManager, "bucketManager must not be null");
        this.downloadUrlProvider = java.util.Objects.requireNonNull(downloadUrlProvider, "downloadUrlProvider must not be null");
    }

    @Override
    public ObjectWriteResult put(ObjectPath path, InputStream content, ObjectMetadata metadata) {
        path = requirePath(path);
        content = requireContent(content);
        ObjectMetadata safeMetadata = safeMetadata(metadata);

        try {
            String token = auth.uploadToken(path.bucket());
            String mime = safeMetadata.contentType();
            Response response = uploadManager.put(content, path.key(), token, null, mime);
            return new ObjectWriteResult(path, response.isOK() ? response.reqId : null, null);
        } catch (QiniuException e) {
            throw objectFailure("put", path, e);
        }
    }

    @Override
    public StorageObject get(ObjectPath path) {
        path = requirePath(path);
        try {
            FileInfo info = bucketManager.stat(path.bucket(), path.key());
            ObjectMetadata metadata = ObjectMetadataMapper.from(
                    () -> info.mimeType,
                    () -> info.fsize,
                    () -> null,
                    () -> null,
                    java.util.Map::of
            );

            String url = downloadUrlProvider.resolve(path.bucket(), path.key());

            InputStream stream = new URL(url).openStream();
            return ObjectStorageValueFactory.storageObject(path, metadata, stream);
        } catch (QiniuException e) {
            throw objectFailure("get", path, e);
        } catch (Exception e) {
            throw objectFailure("get", path, e);
        }
    }

    @Override
    public boolean delete(ObjectPath path) {
        path = requirePath(path);
        try {
            bucketManager.delete(path.bucket(), path.key());
            return true;
        } catch (QiniuException e) {
            throw objectFailure("delete", path, e);
        }
    }

    @Override
    public boolean exists(ObjectPath path) {
        path = requirePath(path);
        try {
            bucketManager.stat(path.bucket(), path.key());
            return true;
        } catch (QiniuException e) {
            if (e.code() == 612) {
                return false;
            }
            throw objectFailure("check object existence", path, e);
        }
    }

    @Override
    public ListObjectsPage list(ListObjectsRequest request) {
        ListObjectsRequest validatedRequest = requireListRequest(request);
        try {
            FileListing listing = bucketManager.listFiles(validatedRequest.bucket(), validatedRequest.prefix(),
                    validatedRequest.continuationToken(), validatedRequest.maxKeys(), null);
            List<ObjectSummary> summaries = List.of();
            if (listing.items != null) {
                summaries = java.util.Arrays.stream(listing.items)
                        .map(item -> ObjectStorageValueFactory.objectSummary(
                                validatedRequest.bucket(),
                                item.key,
                                item.fsize,
                                item.putTime == 0 ? null : Instant.ofEpochMilli(item.putTime / 10000),
                                item.hash))
                        .toList();
            }
            boolean truncated = listing.marker != null && !listing.marker.isBlank();
            return new ListObjectsPage(summaries, listing.marker, truncated);
        } catch (QiniuException e) {
            throw bucketFailure("list", validatedRequest.bucket(), e);
        }
    }

    @Override
    public ObjectWriteResult copy(ObjectPath source, ObjectPath target, ObjectMetadata metadataOverride) {
        source = requirePath(source);
        target = requirePath(target);
        try {
            bucketManager.copy(source.bucket(), source.key(), target.bucket(), target.key());
            return new ObjectWriteResult(target, null, null);
        } catch (QiniuException e) {
            throw objectFailure("copy", source, e);
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
        return super.unwrap(type);
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
