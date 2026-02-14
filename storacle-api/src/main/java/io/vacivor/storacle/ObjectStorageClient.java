package io.vacivor.storacle;

import java.io.InputStream;

public interface ObjectStorageClient extends AutoCloseable {
    ObjectWriteResult put(ObjectPath path, InputStream content, ObjectMetadata metadata);

    StorageObject get(ObjectPath path);

    boolean delete(ObjectPath path);

    boolean exists(ObjectPath path);

    ListObjectsPage list(ListObjectsRequest request);

    ObjectWriteResult copy(ObjectPath source, ObjectPath target, ObjectMetadata metadataOverride);

    default <T> T unwrap(Class<T> type) {
        if (type == null) {
            throw new IllegalArgumentException("type must not be null");
        }
        if (type.isInstance(this)) {
            return type.cast(this);
        }
        throw new IllegalArgumentException("Unsupported native client type: " + type.getName());
    }

    @Override
    void close();
}
