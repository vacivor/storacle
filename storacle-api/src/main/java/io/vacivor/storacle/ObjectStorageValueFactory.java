package io.vacivor.storacle;

import java.io.InputStream;
import java.time.Instant;

public final class ObjectStorageValueFactory {
    private ObjectStorageValueFactory() {
    }

    public static StorageObject storageObject(ObjectPath path, ObjectMetadata metadata, InputStream content) {
        return new StorageObject(path, metadata, content);
    }

    public static ObjectSummary objectSummary(String bucket, String key, long size, Instant lastModified, String eTag) {
        return new ObjectSummary(ObjectPath.of(bucket, key), size, lastModified, eTag);
    }
}
