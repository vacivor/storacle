package io.vacivor.storacle.template;

import io.vacivor.storacle.ListObjectsPage;
import io.vacivor.storacle.ListObjectsRequest;
import io.vacivor.storacle.ChecksumAlgorithm;
import io.vacivor.storacle.ObjectMetadata;
import io.vacivor.storacle.ObjectPath;
import io.vacivor.storacle.ObjectStorageClient;
import io.vacivor.storacle.ObjectWriteResult;
import io.vacivor.storacle.ObjectStorageValueFactory;
import io.vacivor.storacle.StorageObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ObjectStorageTemplateUploadTest {
    @Test
    void uploadClosesRequestContent() {
        AtomicBoolean closed = new AtomicBoolean(false);
        InputStream input = new CloseTrackingInputStream(new ByteArrayInputStream("hello".getBytes()), closed);
        UploadRequest request = UploadRequest.builder()
                .bucket("bucket")
                .objectKey("key")
                .content(input)
                .contentLength(5)
                .build();

        ObjectStorageTemplate template = new ObjectStorageTemplate(new ReadingClient());
        template.upload(request);

        assertTrue(closed.get());
    }

    @Test
    void uploadReturnsRequestedHashes() {
        UploadRequest request = UploadRequest.builder()
                .bucket("bucket")
                .objectKey("key")
                .content("hello".getBytes())
                .addChecksumAlgorithm(ChecksumAlgorithm.SHA_256)
                .addChecksumAlgorithm(ChecksumAlgorithm.CRC32)
                .build();

        ObjectStorageTemplate template = new ObjectStorageTemplate(new ReadingClient());
        ObjectWriteResult result = template.upload(request);

        assertEquals("key", result.path().key());
        assertNull(result.originalFilename());
        assertEquals("application/octet-stream", result.contentType());
        assertEquals(5L, result.contentLength());
        assertEquals("application/octet-stream", result.metadata().contentType());
        assertEquals(5L, result.metadata().contentLength());
        assertEquals("3610a686", result.metadata().userMetadata().get("checksum-crc32"));
        assertEquals(Map.of(
                ChecksumAlgorithm.SHA_256, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                ChecksumAlgorithm.CRC32, "3610a686"
        ), result.checksums());
    }

    @Test
    void uploadByteArraySupportsMetadataAndChecksums() {
        RecordingStorageClient client = new RecordingStorageClient();
        ObjectStorageTemplate template = new ObjectStorageTemplate(client);

        ObjectWriteResult result = template.upload(
                "bucket",
                "hello.txt",
                "demo/",
                "hello".getBytes(),
                ObjectMetadata.builder().cacheControl("no-cache").build(),
                java.util.List.of(ChecksumAlgorithm.CRC32)
        );

        assertEquals("hello.txt", result.originalFilename());
        assertEquals("text/plain", result.contentType());
        assertEquals(5L, result.contentLength());
        assertEquals("text/plain", result.metadata().contentType());
        assertEquals(5L, result.metadata().contentLength());
        assertEquals("3610a686", result.metadata().userMetadata().get("checksum-crc32"));
        assertEquals("no-cache", client.lastMetadata.cacheControl());
        assertEquals("3610a686", result.checksums().get(ChecksumAlgorithm.CRC32));
    }

    @Test
    void templateConvenienceMethodsDelegateToClient(@TempDir Path tempDir) throws Exception {
        RecordingStorageClient client = new RecordingStorageClient();
        ObjectStorageTemplate template = new ObjectStorageTemplate(client);
        Path downloadPath = tempDir.resolve("download.txt");

        client.nextObject = ObjectStorageValueFactory.storageObject(
                ObjectPath.of("bucket", "demo.txt"),
                ObjectMetadata.empty(),
                new ByteArrayInputStream("content".getBytes())
        );

        assertTrue(template.exists("bucket", "demo.txt"));
        assertTrue(template.delete("bucket", "demo.txt"));
        assertFalse(template.delete("bucket", "another.txt"));
        assertEquals("content", new String(template.getBytes("bucket", "demo.txt")));

        client.nextObject = ObjectStorageValueFactory.storageObject(
                ObjectPath.of("bucket", "demo.txt"),
                ObjectMetadata.empty(),
                new ByteArrayInputStream("content".getBytes())
        );
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        template.download("bucket", "demo.txt", output);
        assertEquals("content", output.toString());

        client.nextObject = ObjectStorageValueFactory.storageObject(
                ObjectPath.of("bucket", "demo.txt"),
                ObjectMetadata.empty(),
                new ByteArrayInputStream("content".getBytes())
        );
        template.download("bucket", "demo.txt", downloadPath.toFile());
        assertEquals("content", Files.readString(downloadPath));

        assertEquals("target.txt", template.copy("source", "a.txt", "target", "target.txt").path().key());
        assertEquals("bucket", template.list("bucket", "demo/").objects().getFirst().path().bucket());
    }

    private static final class ReadingClient implements ObjectStorageClient {
        @Override
        public ObjectWriteResult put(ObjectPath path, InputStream content, ObjectMetadata metadata) {
            try {
                content.readAllBytes();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return new ObjectWriteResult(path, null, null);
        }

        @Override
        public StorageObject get(ObjectPath path) {
            throw new UnsupportedOperationException("not used in test");
        }

        @Override
        public boolean delete(ObjectPath path) {
            throw new UnsupportedOperationException("not used in test");
        }

        @Override
        public boolean exists(ObjectPath path) {
            throw new UnsupportedOperationException("not used in test");
        }

        @Override
        public ListObjectsPage list(ListObjectsRequest request) {
            throw new UnsupportedOperationException("not used in test");
        }

        @Override
        public ObjectWriteResult copy(ObjectPath source, ObjectPath target, ObjectMetadata metadataOverride) {
            throw new UnsupportedOperationException("not used in test");
        }

        @Override
        public void close() {
        }
    }

    private static final class RecordingStorageClient implements ObjectStorageClient {
        private StorageObject nextObject;
        private ObjectMetadata lastMetadata;
        private int deleteCalls;

        @Override
        public ObjectWriteResult put(ObjectPath path, InputStream content, ObjectMetadata metadata) {
            this.lastMetadata = metadata;
            try {
                content.readAllBytes();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return new ObjectWriteResult(path, "etag", null);
        }

        @Override
        public StorageObject get(ObjectPath path) {
            return nextObject;
        }

        @Override
        public boolean delete(ObjectPath path) {
            deleteCalls++;
            return deleteCalls == 1;
        }

        @Override
        public boolean exists(ObjectPath path) {
            return true;
        }

        @Override
        public ListObjectsPage list(ListObjectsRequest request) {
            return new ListObjectsPage(
                    java.util.List.of(new io.vacivor.storacle.ObjectSummary(
                            ObjectPath.of(request.bucket(), "demo.txt"),
                            7,
                            null,
                            "etag"
                    )),
                    null,
                    false
            );
        }

        @Override
        public ObjectWriteResult copy(ObjectPath source, ObjectPath target, ObjectMetadata metadataOverride) {
            return new ObjectWriteResult(target, "etag", null);
        }

        @Override
        public void close() {
        }
    }

    private static final class CloseTrackingInputStream extends InputStream {
        private final InputStream delegate;
        private final AtomicBoolean closed;

        private CloseTrackingInputStream(InputStream delegate, AtomicBoolean closed) {
            this.delegate = delegate;
            this.closed = closed;
        }

        @Override
        public int read() throws IOException {
            return delegate.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return delegate.read(b, off, len);
        }

        @Override
        public void close() throws IOException {
            closed.set(true);
            delegate.close();
        }
    }
}
