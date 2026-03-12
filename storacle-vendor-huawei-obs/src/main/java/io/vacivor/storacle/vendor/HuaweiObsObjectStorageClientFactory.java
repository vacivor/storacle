package io.vacivor.storacle.vendor;

import com.obs.services.ObsClient;
import io.vacivor.storacle.ObjectStorageClient;
import io.vacivor.storacle.StorageVendorConfig;
import io.vacivor.storacle.client.HuaweiObsStorageClient;

public final class HuaweiObsObjectStorageClientFactory extends AbstractObjectStorageClientFactory {
    public HuaweiObsObjectStorageClientFactory() {
        super(StorageVendor.HUAWEI_OBS);
    }

    @Override
    public ObjectStorageClient create(StorageVendorConfig config) {
        assertVendor(config);

        String endpoint = requireNonBlank(config.endpoint(), "endpoint");
        String accessKey = requireNonBlank(config.accessKey(), "accessKey");
        String secretKey = requireNonBlank(config.secretKey(), "secretKey");

        ObsClient client = new ObsClient(accessKey, secretKey, endpoint);
        return new HuaweiObsStorageClient(client);
    }
}
