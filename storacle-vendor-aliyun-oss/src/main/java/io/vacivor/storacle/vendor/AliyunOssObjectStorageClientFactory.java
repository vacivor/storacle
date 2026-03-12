package io.vacivor.storacle.vendor;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import io.vacivor.storacle.ObjectStorageClient;
import io.vacivor.storacle.StorageVendorConfig;
import io.vacivor.storacle.client.AliyunOssStorageClient;

public final class AliyunOssObjectStorageClientFactory extends AbstractObjectStorageClientFactory {
    public AliyunOssObjectStorageClientFactory() {
        super(StorageVendor.ALIYUN_OSS);
    }

    @Override
    public ObjectStorageClient create(StorageVendorConfig config) {
        assertVendor(config);

        String endpoint = requireNonBlank(config.endpoint(), "endpoint");
        String accessKey = requireNonBlank(config.accessKey(), "accessKey");
        String secretKey = requireNonBlank(config.secretKey(), "secretKey");

        OSS client = new OSSClientBuilder().build(endpoint, accessKey, secretKey);
        return new AliyunOssStorageClient(client);
    }
}
