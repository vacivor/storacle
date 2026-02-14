package io.vacivor.storacle.vendor;

import io.vacivor.storacle.ObjectStorageClient;
import io.vacivor.storacle.StorageVendorConfig;

public interface ObjectStorageClientFactory {
    StorageVendor vendor();

    ObjectStorageClient create(StorageVendorConfig config);
}
