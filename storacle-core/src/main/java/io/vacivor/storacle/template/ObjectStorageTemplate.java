package io.vacivor.storacle.template;

import java.util.Objects;

import io.vacivor.storacle.ContentTypeDetector;
import io.vacivor.storacle.DefaultContentTypeDetector;
import io.vacivor.storacle.FilenameContext;
import io.vacivor.storacle.FilenameGenerator;
import io.vacivor.storacle.ChecksumAlgorithm;
import io.vacivor.storacle.ChecksumInputStream;
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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

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

        InputStream originalContent = request.content();
        try {
            byte[] prefix = originalContent.readNBytes(request.peekBytes());
            try (InputStream content = new SequenceInputStream(new ByteArrayInputStream(prefix), originalContent)) {
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
                Set<ChecksumAlgorithm> checksumAlgorithms = request.checksumAlgorithms();
                if (checksumAlgorithms.isEmpty()) {
                    ObjectWriteResult result = client.put(ObjectPath.of(request.bucket(), objectKey), content, resolvedMetadata);
                    return enrichWriteResult(result, request.originalFilename(), resolvedMetadata, result.checksums());
                }
                try (ChecksumInputStream checksumContent = new ChecksumInputStream(content, checksumAlgorithms)) {
                    ObjectWriteResult result = client.put(ObjectPath.of(request.bucket(), objectKey), checksumContent, resolvedMetadata);
                    Map<ChecksumAlgorithm, String> checksums = new LinkedHashMap<>(result.checksums());
                    checksums.putAll(checksumContent.checksums());
                    return enrichWriteResult(result, request.originalFilename(), resolvedMetadata, checksums);
                }
            }
        } catch (IOException e) {
            try {
                originalContent.close();
            } catch (IOException ignored) {
                // Best effort.
            }
            throw new StorageException("Failed to read upload content", e);
        }
    }

    public ObjectWriteResult upload(ObjectPath path, InputStream content, ObjectMetadata metadata) {
        Objects.requireNonNull(path, "path must not be null");
        return client.put(path, content, metadata);
    }

    public ObjectWriteResult upload(String bucket, String objectKey, InputStream content, ObjectMetadata metadata) {
        return client.put(bucket, objectKey, content, metadata);
    }

    public ObjectWriteResult upload(ObjectPath path, InputStream content) {
        Objects.requireNonNull(path, "path must not be null");
        return client.put(path, content);
    }

    public ObjectWriteResult upload(String bucket, String objectKey, InputStream content) {
        return client.put(bucket, objectKey, content);
    }

    public ObjectWriteResult upload(ObjectPath path, byte[] content, ObjectMetadata metadata) {
        Objects.requireNonNull(path, "path must not be null");
        return client.put(path, content, metadata);
    }

    public ObjectWriteResult upload(String bucket, String objectKey, byte[] content, ObjectMetadata metadata) {
        return client.put(bucket, objectKey, content, metadata);
    }

    public ObjectWriteResult upload(ObjectPath path, byte[] content) {
        Objects.requireNonNull(path, "path must not be null");
        return client.put(path, content);
    }

    public ObjectWriteResult upload(String bucket, String objectKey, byte[] content) {
        return client.put(bucket, objectKey, content);
    }

    public ObjectWriteResult upload(ObjectPath path, File file, ObjectMetadata metadata) {
        Objects.requireNonNull(path, "path must not be null");
        return client.put(path, file, metadata);
    }

    public ObjectWriteResult upload(String bucket, String objectKey, File file, ObjectMetadata metadata) {
        return client.put(bucket, objectKey, file, metadata);
    }

    public ObjectWriteResult upload(ObjectPath path, File file) {
        Objects.requireNonNull(path, "path must not be null");
        return client.put(path, file);
    }

    public ObjectWriteResult upload(String bucket, String objectKey, File file) {
        return client.put(bucket, objectKey, file);
    }

    public ObjectWriteResult upload(String bucket, String originalFilename, String prefix, byte[] bytes) {
        return upload(bucket, originalFilename, prefix, bytes, null, Set.of());
    }

    public ObjectWriteResult upload(String bucket, String originalFilename, String prefix, byte[] bytes,
                                    ObjectMetadata metadata) {
        return upload(bucket, originalFilename, prefix, bytes, metadata, Set.of());
    }

    public ObjectWriteResult upload(String bucket, String originalFilename, String prefix, byte[] bytes,
                                    Collection<ChecksumAlgorithm> checksumAlgorithms) {
        return upload(bucket, originalFilename, prefix, bytes, null, checksumAlgorithms);
    }

    public ObjectWriteResult upload(String bucket, String originalFilename, String prefix, byte[] bytes,
                                    ObjectMetadata metadata, Collection<ChecksumAlgorithm> checksumAlgorithms) {
        Objects.requireNonNull(bytes, "bytes must not be null");
        return upload(buildUploadRequest(
                bucket,
                originalFilename,
                prefix,
                metadata,
                checksumAlgorithms,
                builder -> builder.content(bytes)
        ));
    }

    public ObjectWriteResult upload(String bucket, String originalFilename, String prefix, File file) {
        return upload(bucket, originalFilename, prefix, file, null, Set.of());
    }

    public ObjectWriteResult upload(String bucket, String originalFilename, String prefix, File file,
                                    ObjectMetadata metadata) {
        return upload(bucket, originalFilename, prefix, file, metadata, Set.of());
    }

    public ObjectWriteResult upload(String bucket, String originalFilename, String prefix, File file,
                                    Collection<ChecksumAlgorithm> checksumAlgorithms) {
        return upload(bucket, originalFilename, prefix, file, null, checksumAlgorithms);
    }

    public ObjectWriteResult upload(String bucket, String originalFilename, String prefix, File file,
                                    ObjectMetadata metadata, Collection<ChecksumAlgorithm> checksumAlgorithms) {
        Objects.requireNonNull(file, "file must not be null");
        return upload(buildUploadRequest(
                bucket,
                originalFilename != null ? originalFilename : file.getName(),
                prefix,
                metadata,
                checksumAlgorithms,
                builder -> builder.content(file)
        ));
    }

    public StorageObject get(ObjectPath path) {
        Objects.requireNonNull(path, "path must not be null");
        return client.get(path);
    }

    public StorageObject get(String bucket, String key) {
        return client.get(bucket, key);
    }

    public byte[] getBytes(ObjectPath path) {
        return client.getBytes(path);
    }

    public byte[] getBytes(String bucket, String key) {
        return client.getBytes(bucket, key);
    }

    public void download(ObjectPath path, OutputStream output) {
        Objects.requireNonNull(path, "path must not be null");
        client.download(path, output);
    }

    public void download(String bucket, String key, OutputStream output) {
        client.download(bucket, key, output);
    }

    public void download(ObjectPath path, File file) {
        Objects.requireNonNull(path, "path must not be null");
        client.download(path, file);
    }

    public void download(String bucket, String key, File file) {
        client.download(bucket, key, file);
    }

    public boolean exists(ObjectPath path) {
        Objects.requireNonNull(path, "path must not be null");
        return client.exists(path);
    }

    public boolean exists(String bucket, String key) {
        return client.exists(bucket, key);
    }

    public boolean delete(ObjectPath path) {
        Objects.requireNonNull(path, "path must not be null");
        return client.delete(path);
    }

    public boolean delete(String bucket, String key) {
        return client.delete(bucket, key);
    }

    public int delete(Iterable<ObjectPath> paths) {
        return client.delete(paths);
    }

    public ListObjectsPage list(ListObjectsRequest request) {
        Objects.requireNonNull(request, "request must not be null");
        return client.list(request);
    }

    public ListObjectsPage list(String bucket, String prefix, int maxKeys, String continuationToken) {
        return client.list(bucket, prefix, maxKeys, continuationToken);
    }

    public ListObjectsPage list(String bucket, String prefix, int maxKeys) {
        return client.list(bucket, prefix, maxKeys, null);
    }

    public ListObjectsPage list(String bucket, String prefix) {
        return client.list(bucket, prefix, 1000, null);
    }

    public ObjectWriteResult copy(ObjectPath source, ObjectPath target, ObjectMetadata metadataOverride) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(target, "target must not be null");
        return client.copy(source, target, metadataOverride);
    }

    public ObjectWriteResult copy(ObjectPath source, ObjectPath target) {
        return client.copy(source, target);
    }

    public ObjectWriteResult copy(String sourceBucket, String sourceKey, String targetBucket, String targetKey,
                                  ObjectMetadata metadataOverride) {
        return client.copy(sourceBucket, sourceKey, targetBucket, targetKey, metadataOverride);
    }

    public ObjectWriteResult copy(String sourceBucket, String sourceKey, String targetBucket, String targetKey) {
        return client.copy(sourceBucket, sourceKey, targetBucket, targetKey);
    }

    public ObjectStorageClient objectStorageClient() {
        return client;
    }

    private ObjectWriteResult enrichWriteResult(ObjectWriteResult result, String originalFilename,
                                                ObjectMetadata metadata, Map<ChecksumAlgorithm, String> checksums) {
        return new ObjectWriteResult(
                result.path(),
                originalFilename,
                metadata.contentType(),
                metadata.contentLength(),
                result.eTag(),
                result.versionId(),
                checksums
        );
    }

    private UploadRequest buildUploadRequest(String bucket, String originalFilename, String prefix,
                                             ObjectMetadata metadata, Collection<ChecksumAlgorithm> checksumAlgorithms,
                                             java.util.function.Consumer<UploadRequest.Builder> contentConfigurer) {
        UploadRequest.Builder builder = UploadRequest.builder()
                .bucket(bucket)
                .originalFilename(originalFilename)
                .prefix(prefix)
                .metadata(metadata)
                .checksumAlgorithms(checksumAlgorithms);
        contentConfigurer.accept(builder);
        return builder.build();
    }
}
