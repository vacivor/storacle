package io.vacivor.storacle.vendor;

import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;
import io.vacivor.storacle.ObjectStorageClient;
import io.vacivor.storacle.StorageVendorConfig;
import io.vacivor.storacle.client.BaiduBosStorageClient;

public final class BaiduBosObjectStorageClientFactory extends AbstractObjectStorageClientFactory {
    public BaiduBosObjectStorageClientFactory() {
        super(StorageVendor.BAIDU_BOS);
    }

    @Override
    public ObjectStorageClient create(StorageVendorConfig config) {
        assertVendor(config);

        String endpoint = requireNonBlank(config.endpoint(), "endpoint");
        String accessKey = requireNonBlank(config.accessKey(), "accessKey");
        String secretKey = requireNonBlank(config.secretKey(), "secretKey");

        BosClientConfiguration configuration = new BosClientConfiguration();
        configuration.setCredentials(new DefaultBceCredentials(accessKey, secretKey));
        configuration.setEndpoint(endpoint);

        BosClient client = new BosClient(configuration);
        return new BaiduBosStorageClient(client);
    }
}
