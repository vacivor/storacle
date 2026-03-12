package io.vacivor.storacle.vendor;

import io.vacivor.storacle.ListObjectsPage;
import io.vacivor.storacle.ListObjectsRequest;
import io.vacivor.storacle.ObjectMetadata;
import io.vacivor.storacle.ObjectPath;
import io.vacivor.storacle.ObjectStorageClient;
import io.vacivor.storacle.ObjectWriteResult;
import io.vacivor.storacle.StorageObject;
import io.vacivor.storacle.StorageVendorConfig;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ObjectStorageClientFactoryRegistryTest {
    @Test
    void createUsesVendorFromConfig() {
        TestStorageClient expectedClient = new TestStorageClient();
        StorageVendorConfig config = StorageVendorConfig.builder(StorageVendor.MINIO).build();
        ObjectStorageClientFactoryRegistry registry = new ObjectStorageClientFactoryRegistry(List.of(
                new TestFactory(StorageVendor.MINIO, expectedClient)
        ));

        ObjectStorageClient actualClient = registry.create(config);

        assertSame(expectedClient, actualClient);
    }

    @Test
    void constructorRejectsDuplicateFactories() {
        assertThrows(IllegalArgumentException.class, () -> new ObjectStorageClientFactoryRegistry(List.of(
                new TestFactory(StorageVendor.MINIO, new TestStorageClient()),
                new TestFactory(StorageVendor.MINIO, new TestStorageClient())
        )));
    }

    @Test
    void createRejectsMissingVendorFactory() {
        ObjectStorageClientFactoryRegistry registry = new ObjectStorageClientFactoryRegistry(List.of());

        assertThrows(IllegalArgumentException.class,
                () -> registry.create(StorageVendorConfig.builder(StorageVendor.MINIO).build()));
    }

    private static final class TestFactory extends AbstractObjectStorageClientFactory {
        private final ObjectStorageClient client;

        private TestFactory(StorageVendor vendor, ObjectStorageClient client) {
            super(vendor);
            this.client = client;
        }

        @Override
        public ObjectStorageClient create(StorageVendorConfig config) {
            assertVendor(config);
            return client;
        }
    }

    private static final class TestStorageClient implements ObjectStorageClient {
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
    }
}
