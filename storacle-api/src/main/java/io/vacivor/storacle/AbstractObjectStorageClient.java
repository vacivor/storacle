package io.vacivor.storacle;

import java.io.InputStream;
import java.util.Objects;

public abstract class AbstractObjectStorageClient implements ObjectStorageClient {
    protected final ObjectPath requirePath(ObjectPath path) {
        return Objects.requireNonNull(path, "path must not be null");
    }

    protected final InputStream requireContent(InputStream content) {
        return Objects.requireNonNull(content, "content must not be null");
    }

    protected final ListObjectsRequest requireListRequest(ListObjectsRequest request) {
        return Objects.requireNonNull(request, "request must not be null");
    }

    protected final ObjectMetadata safeMetadata(ObjectMetadata metadata) {
        return metadata == null ? ObjectMetadata.empty() : metadata;
    }

    protected final StorageException objectFailure(String action, ObjectPath path, Exception cause) {
        return new StorageException("Failed to " + action + " object: " + path.bucket() + "/" + path.key(), cause);
    }

    protected final StorageException bucketFailure(String action, String bucket, Exception cause) {
        return new StorageException("Failed to " + action + " objects for bucket: " + bucket, cause);
    }
}
