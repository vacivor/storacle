package io.vacivor.storacle.vendor;

import io.vacivor.storacle.ObjectStorageClient;
import io.vacivor.storacle.StorageVendorConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AbstractObjectStorageClientFactoryTest {
    @Test
    void requireNonBlankRejectsBlankValue() {
        TestFactory factory = new TestFactory();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> factory.require(" ", "accessKey"));

        assertEquals("accessKey must not be blank", exception.getMessage());
    }

    @Test
    void hasTextMatchesNonBlankContract() {
        TestFactory factory = new TestFactory();

        assertFalse(factory.hasTextValue(null));
        assertFalse(factory.hasTextValue(" "));
        assertTrue(factory.hasTextValue("minio"));
    }

    private static final class TestFactory extends AbstractObjectStorageClientFactory {
        private TestFactory() {
            super(StorageVendor.MINIO);
        }

        @Override
        public ObjectStorageClient create(StorageVendorConfig config) {
            throw new UnsupportedOperationException("not used in test");
        }

        private String require(String value, String field) {
            return requireNonBlank(value, field);
        }

        private boolean hasTextValue(String value) {
            return hasText(value);
        }
    }
}
