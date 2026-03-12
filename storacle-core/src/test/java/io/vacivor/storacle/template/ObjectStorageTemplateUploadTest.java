package io.vacivor.storacle.template;

import io.vacivor.storacle.ListObjectsPage;
import io.vacivor.storacle.ListObjectsRequest;
import io.vacivor.storacle.ObjectMetadata;
import io.vacivor.storacle.ObjectPath;
import io.vacivor.storacle.ObjectStorageClient;
import io.vacivor.storacle.ObjectWriteResult;
import io.vacivor.storacle.StorageObject;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

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
