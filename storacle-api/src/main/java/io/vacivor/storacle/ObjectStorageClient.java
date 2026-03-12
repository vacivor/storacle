package io.vacivor.storacle;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Objects;

public interface ObjectStorageClient extends AutoCloseable {
    ObjectWriteResult put(ObjectPath path, InputStream content, ObjectMetadata metadata);

    default ObjectWriteResult put(String bucket, String key, InputStream content, ObjectMetadata metadata) {
        Objects.requireNonNull(content, "content must not be null");
        return put(ObjectPath.of(bucket, key), content, metadata);
    }

    default ObjectWriteResult put(ObjectPath path, InputStream content) {
        return put(path, content, ObjectMetadata.empty());
    }

    default ObjectWriteResult put(String bucket, String key, InputStream content) {
        return put(bucket, key, content, ObjectMetadata.empty());
    }

    default ObjectWriteResult put(ObjectPath path, byte[] content, ObjectMetadata metadata) {
        Objects.requireNonNull(content, "content must not be null");
        ObjectMetadata resolvedMetadata = metadata == null
                ? ObjectMetadata.builder().contentLength((long) content.length).build()
                : ObjectMetadata.builder(metadata).contentLength((long) content.length).build();
        return put(path, new ByteArrayInputStream(content), resolvedMetadata);
    }

    default ObjectWriteResult put(String bucket, String key, byte[] content, ObjectMetadata metadata) {
        return put(ObjectPath.of(bucket, key), content, metadata);
    }

    default ObjectWriteResult put(ObjectPath path, byte[] content) {
        return put(path, content, ObjectMetadata.empty());
    }

    default ObjectWriteResult put(String bucket, String key, byte[] content) {
        return put(bucket, key, content, ObjectMetadata.empty());
    }

    default ObjectWriteResult put(ObjectPath path, File file, ObjectMetadata metadata) {
        Objects.requireNonNull(file, "file must not be null");
        try (InputStream content = new FileInputStream(file)) {
            ObjectMetadata resolvedMetadata = metadata == null
                    ? ObjectMetadata.builder().contentLength(file.length()).build()
                    : ObjectMetadata.builder(metadata).contentLength(file.length()).build();
            return put(path, content, resolvedMetadata);
        } catch (IOException e) {
            throw new StorageException("Failed to read upload file: " + file.getAbsolutePath(), e);
        }
    }

    default ObjectWriteResult put(String bucket, String key, File file, ObjectMetadata metadata) {
        return put(ObjectPath.of(bucket, key), file, metadata);
    }

    default ObjectWriteResult put(ObjectPath path, File file) {
        return put(path, file, ObjectMetadata.empty());
    }

    default ObjectWriteResult put(String bucket, String key, File file) {
        return put(bucket, key, file, ObjectMetadata.empty());
    }

    StorageObject get(ObjectPath path);

    default StorageObject get(String bucket, String key) {
        return get(ObjectPath.of(bucket, key));
    }

    default byte[] getBytes(ObjectPath path) {
        try (StorageObject object = get(path); InputStream content = object.content()) {
            return content.readAllBytes();
        } catch (IOException e) {
            throw new StorageException("Failed to read object content: " + path.bucket() + "/" + path.key(), e);
        }
    }

    default byte[] getBytes(String bucket, String key) {
        return getBytes(ObjectPath.of(bucket, key));
    }

    default void download(ObjectPath path, OutputStream output) {
        Objects.requireNonNull(output, "output must not be null");
        try (StorageObject object = get(path); InputStream content = object.content()) {
            content.transferTo(output);
        } catch (IOException e) {
            throw new StorageException("Failed to write object content: " + path.bucket() + "/" + path.key(), e);
        }
    }

    default void download(String bucket, String key, OutputStream output) {
        download(ObjectPath.of(bucket, key), output);
    }

    default void download(ObjectPath path, File file) {
        Objects.requireNonNull(file, "file must not be null");
        try (OutputStream output = new FileOutputStream(file)) {
            download(path, output);
        } catch (IOException e) {
            throw new StorageException("Failed to write download file: " + file.getAbsolutePath(), e);
        }
    }

    default void download(String bucket, String key, File file) {
        download(ObjectPath.of(bucket, key), file);
    }

    boolean delete(ObjectPath path);

    default boolean delete(String bucket, String key) {
        return delete(ObjectPath.of(bucket, key));
    }

    default int delete(Iterable<ObjectPath> paths) {
        Objects.requireNonNull(paths, "paths must not be null");
        int deleted = 0;
        for (ObjectPath path : paths) {
            if (path != null && delete(path)) {
                deleted++;
            }
        }
        return deleted;
    }

    default int delete(Collection<ObjectPath> paths) {
        return delete((Iterable<ObjectPath>) paths);
    }

    boolean exists(ObjectPath path);

    default boolean exists(String bucket, String key) {
        return exists(ObjectPath.of(bucket, key));
    }

    ListObjectsPage list(ListObjectsRequest request);

    default ListObjectsPage list(String bucket, String prefix, int maxKeys, String continuationToken) {
        return list(ListObjectsRequest.builder()
                .bucket(bucket)
                .prefix(prefix)
                .maxKeys(maxKeys)
                .continuationToken(continuationToken)
                .build());
    }

    ObjectWriteResult copy(ObjectPath source, ObjectPath target, ObjectMetadata metadataOverride);

    default ObjectWriteResult copy(ObjectPath source, ObjectPath target) {
        return copy(source, target, null);
    }

    default ObjectWriteResult copy(String sourceBucket, String sourceKey, String targetBucket, String targetKey,
                                   ObjectMetadata metadataOverride) {
        return copy(ObjectPath.of(sourceBucket, sourceKey), ObjectPath.of(targetBucket, targetKey), metadataOverride);
    }

    default ObjectWriteResult copy(String sourceBucket, String sourceKey, String targetBucket, String targetKey) {
        return copy(sourceBucket, sourceKey, targetBucket, targetKey, null);
    }

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
