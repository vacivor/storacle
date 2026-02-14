package io.vacivor.storacle.template;

import java.util.Objects;

import io.vacivor.storacle.ContentTypeDetector;
import io.vacivor.storacle.DefaultContentTypeDetector;
import io.vacivor.storacle.FilenameContext;
import io.vacivor.storacle.FilenameGenerator;
import io.vacivor.storacle.ListObjectsPage;
import io.vacivor.storacle.ListObjectsRequest;
import io.vacivor.storacle.ObjectMetadata;
import io.vacivor.storacle.ObjectPath;
import io.vacivor.storacle.ObjectStorageClient;
import io.vacivor.storacle.ObjectWriteResult;
import io.vacivor.storacle.StorageObject;
import io.vacivor.storacle.StorageException;
import io.vacivor.storacle.UuidFilenameGenerator;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;

public final class ObjectStorageTemplate {
    private final ObjectStorageClient client;
    private final ContentTypeDetector contentTypeDetector;
    private final FilenameGenerator filenameGenerator;

    public ObjectStorageTemplate(ObjectStorageClient client) {
        this(client, new DefaultContentTypeDetector(), new UuidFilenameGenerator());
    }

    public ObjectStorageTemplate(ObjectStorageClient client, ContentTypeDetector contentTypeDetector,
                            FilenameGenerator filenameGenerator) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.contentTypeDetector = Objects.requireNonNull(contentTypeDetector, "contentTypeDetector must not be null");
        this.filenameGenerator = Objects.requireNonNull(filenameGenerator, "filenameGenerator must not be null");
    }

    public ObjectWriteResult upload(UploadRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        ObjectMetadata baseMetadata = request.metadata() == null ? ObjectMetadata.empty() : request.metadata();
        String objectKey = request.objectKey();

        try {
            byte[] prefix = request.content().readNBytes(request.peekBytes());
            InputStream content = new SequenceInputStream(new ByteArrayInputStream(prefix), request.content());
            String contentType = baseMetadata.contentType() == null
                    ? contentTypeDetector.detect(prefix, request.originalFilename())
                    : baseMetadata.contentType();

            ObjectMetadata.Builder metadataBuilder = ObjectMetadata.builder(baseMetadata);
            if (baseMetadata.contentType() == null) {
                metadataBuilder.contentType(contentType);
            }
            if (baseMetadata.contentLength() == null && request.contentLength() >= 0) {
                metadataBuilder.contentLength(request.contentLength());
            }
            ObjectMetadata resolvedMetadata = metadataBuilder.build();

            if (objectKey == null || objectKey.isBlank()) {
                FilenameContext context = FilenameContext.of(request.originalFilename(), request.prefix(), contentType);
                objectKey = filenameGenerator.generate(context);
            }

            return client.put(ObjectPath.of(request.bucket(), objectKey), content, resolvedMetadata);
        } catch (IOException e) {
            throw new StorageException("Failed to read upload content", e);
        }
    }

    public ObjectWriteResult upload(String bucket, String objectKey, InputStream content, ObjectMetadata metadata) {
        Objects.requireNonNull(content, "content must not be null");
        UploadRequest request = UploadRequest.builder()
                .bucket(bucket)
                .objectKey(objectKey)
                .content(content)
                .metadata(metadata)
                .build();
        return upload(request);
    }

    public ObjectWriteResult upload(String bucket, String originalFilename, String prefix, byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes must not be null");
        UploadRequest request = UploadRequest.builder()
                .bucket(bucket)
                .originalFilename(originalFilename)
                .prefix(prefix)
                .content(new ByteArrayInputStream(bytes))
                .contentLength(bytes.length)
                .build();
        return upload(request);
    }

    public ObjectWriteResult upload(String bucket, String originalFilename, String prefix, File file) {
        Objects.requireNonNull(file, "file must not be null");
        try {
            UploadRequest request = UploadRequest.builder()
                    .bucket(bucket)
                    .originalFilename(originalFilename != null ? originalFilename : file.getName())
                    .prefix(prefix)
                    .content(new FileInputStream(file))
                    .contentLength(file.length())
                    .build();
            return upload(request);
        } catch (IOException e) {
            throw new StorageException("Failed to read file for upload", e);
        }
    }

    public StorageObject get(ObjectPath path) {
        Objects.requireNonNull(path, "path must not be null");
        return client.get(path);
    }

    public byte[] getBytes(ObjectPath path) {
        StorageObject object = get(path);
        try (InputStream content = object.content()) {
            return content.readAllBytes();
        } catch (IOException e) {
            throw new StorageException("Failed to read object content", e);
        }
    }

    public boolean exists(ObjectPath path) {
        Objects.requireNonNull(path, "path must not be null");
        return client.exists(path);
    }

    public boolean delete(ObjectPath path) {
        Objects.requireNonNull(path, "path must not be null");
        return client.delete(path);
    }

    public int delete(Iterable<ObjectPath> paths) {
        Objects.requireNonNull(paths, "paths must not be null");
        int deleted = 0;
        for (ObjectPath path : paths) {
            if (path == null) {
                continue;
            }
            if (client.delete(path)) {
                deleted++;
            }
        }
        return deleted;
    }

    public ListObjectsPage list(ListObjectsRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        return client.list(request);
    }

    public ListObjectsPage list(String bucket, String prefix, int maxKeys, String continuationToken) {
        ListObjectsRequest request = ListObjectsRequest.builder()
                .bucket(bucket)
                .prefix(prefix)
                .maxKeys(maxKeys)
                .continuationToken(continuationToken)
                .build();
        return list(request);
    }

    public ObjectWriteResult copy(ObjectPath source, ObjectPath target, ObjectMetadata metadataOverride) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(target, "target must not be null");
        return client.copy(source, target, metadataOverride);
    }

    public ObjectWriteResult copy(ObjectPath source, ObjectPath target) {
        return copy(source, target, null);
    }

    public ObjectStorageClient objectStorageClient() {
        return client;
    }
}
