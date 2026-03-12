package io.vacivor.storacle;

import org.junit.jupiter.api.Test;

import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AbstractObjectStorageClientTest {
    @Test
    void safeMetadataFallsBackToEmptyMetadata() {
        TestStorageClient client = new TestStorageClient();
        ObjectMetadata metadata = client.safe(null);

        assertEquals(null, metadata.contentType());
        assertEquals(null, metadata.contentLength());
        assertEquals(0, metadata.userMetadata().size());
    }

    @Test
    void objectFailureUsesConsistentMessageShape() {
        TestStorageClient client = new TestStorageClient();
        StorageException exception = client.fail("put", ObjectPath.of("bucket", "key"), new IllegalStateException("boom"));

        assertEquals("Failed to put object: bucket/key", exception.getMessage());
    }

    private static final class TestStorageClient extends AbstractObjectStorageClient {
        @Override
        public ObjectWriteResult put(ObjectPath path, InputStream content, ObjectMetadata metadata) {
            throw new UnsupportedOperationException("not used in test");
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

        private ObjectMetadata safe(ObjectMetadata metadata) {
            return safeMetadata(metadata);
        }

        private StorageException fail(String action, ObjectPath path, Exception cause) {
            return objectFailure(action, path, cause);
        }
    }
}
