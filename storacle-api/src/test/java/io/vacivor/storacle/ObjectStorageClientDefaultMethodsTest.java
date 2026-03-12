package io.vacivor.storacle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ObjectStorageClientDefaultMethodsTest {
    @Test
    void putByteArrayWrapsContentAndLength() throws Exception {
        RecordingStorageClient client = new RecordingStorageClient();

        ObjectWriteResult result = client.put("bucket", "demo.txt", "hello".getBytes());

        assertEquals("bucket", result.path().bucket());
        assertEquals("demo.txt", client.lastPath.key());
        assertEquals(5L, client.lastMetadata.contentLength());
        assertArrayEquals("hello".getBytes(), client.lastContent.readAllBytes());
    }

    @Test
    void getBytesReadsFromStorageObject() {
        RecordingStorageClient client = new RecordingStorageClient();
        client.nextObject = ObjectStorageValueFactory.storageObject(
                ObjectPath.of("bucket", "file.txt"),
                ObjectMetadata.empty(),
                new ByteArrayInputStream("data".getBytes())
        );

        byte[] content = client.getBytes("bucket", "file.txt");

        assertArrayEquals("data".getBytes(), content);
    }

    @Test
    void downloadStreamsToOutput() {
        RecordingStorageClient client = new RecordingStorageClient();
        client.nextObject = ObjectStorageValueFactory.storageObject(
                ObjectPath.of("bucket", "file.txt"),
                ObjectMetadata.empty(),
                new ByteArrayInputStream("stream".getBytes())
        );
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        client.download("bucket", "file.txt", output);

        assertArrayEquals("stream".getBytes(), output.toByteArray());
    }

    @Test
    void putFileUsesFileLength(@TempDir Path tempDir) throws Exception {
        RecordingStorageClient client = new RecordingStorageClient();
        Path path = tempDir.resolve("upload.txt");
        Files.writeString(path, "from-file");

        client.put("bucket", "upload.txt", path.toFile());

        assertEquals(9L, client.lastMetadata.contentLength());
        assertArrayEquals("from-file".getBytes(), client.lastBytes);
    }

    @Test
    void downloadToFileWritesContent(@TempDir Path tempDir) throws Exception {
        RecordingStorageClient client = new RecordingStorageClient();
        client.nextObject = ObjectStorageValueFactory.storageObject(
                ObjectPath.of("bucket", "file.txt"),
                ObjectMetadata.empty(),
                new ByteArrayInputStream("saved".getBytes())
        );
        File target = tempDir.resolve("saved.txt").toFile();

        client.download("bucket", "file.txt", target);

        assertEquals("saved", Files.readString(target.toPath()));
    }

    @Test
    void batchDeleteCountsSuccessfulDeletes() {
        RecordingStorageClient client = new RecordingStorageClient();

        int deleted = client.delete(List.of(
                ObjectPath.of("bucket", "a"),
                ObjectPath.of("bucket", "b")
        ));

        assertEquals(1, deleted);
    }

    @Test
    void convenienceMethodsDelegateToCoreMethods() {
        RecordingStorageClient client = new RecordingStorageClient();

        assertTrue(client.exists("bucket", "exists.txt"));
        assertTrue(client.delete("bucket", "missing.txt"));
        assertSame(client.nextListPage, client.list("bucket", "prefix/", 10, "token"));
        assertEquals("target.txt", client.copy("bucket", "source.txt", "bucket", "target.txt").path().key());
    }

    private static final class RecordingStorageClient implements ObjectStorageClient {
        private ObjectPath lastPath;
        private InputStream lastContent;
        private byte[] lastBytes;
        private ObjectMetadata lastMetadata;
        private StorageObject nextObject;
        private final ListObjectsPage nextListPage = new ListObjectsPage(List.of(), "next", true);
        private int deleteCalls;

        @Override
        public ObjectWriteResult put(ObjectPath path, InputStream content, ObjectMetadata metadata) {
            this.lastPath = path;
            this.lastContent = content;
            this.lastMetadata = metadata;
            try {
                this.lastBytes = content.readAllBytes();
                this.lastContent = new ByteArrayInputStream(lastBytes);
            } catch (java.io.IOException e) {
                throw new UncheckedIOException(e);
            }
            return new ObjectWriteResult(path, "etag", null);
        }

        @Override
        public StorageObject get(ObjectPath path) {
            this.lastPath = path;
            return nextObject;
        }

        @Override
        public boolean delete(ObjectPath path) {
            this.lastPath = path;
            deleteCalls++;
            return deleteCalls == 1;
        }

        @Override
        public boolean exists(ObjectPath path) {
            this.lastPath = path;
            return true;
        }

        @Override
        public ListObjectsPage list(ListObjectsRequest request) {
            return nextListPage;
        }

        @Override
        public ObjectWriteResult copy(ObjectPath source, ObjectPath target, ObjectMetadata metadataOverride) {
            return new ObjectWriteResult(target, null, null);
        }

        @Override
        public void close() {
        }
    }
}
