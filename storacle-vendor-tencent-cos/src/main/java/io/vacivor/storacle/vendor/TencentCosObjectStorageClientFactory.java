package io.vacivor.storacle.vendor;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.region.Region;
import io.vacivor.storacle.ObjectStorageClient;
import io.vacivor.storacle.StorageVendorConfig;
import io.vacivor.storacle.client.TencentCosStorageClient;

public final class TencentCosObjectStorageClientFactory extends AbstractObjectStorageClientFactory {
    public TencentCosObjectStorageClientFactory() {
        super(StorageVendor.TENCENT_COS);
    }

    @Override
    public ObjectStorageClient create(StorageVendorConfig config) {
        assertVendor(config);

        String accessKey = requireNonBlank(config.accessKey(), "accessKey");
        String secretKey = requireNonBlank(config.secretKey(), "secretKey");
        String region = requireNonBlank(config.region(), "region");

        COSCredentials credentials = new BasicCOSCredentials(accessKey, secretKey);
        ClientConfig clientConfig = new ClientConfig(new Region(region));
        if (hasText(config.endpoint())) {
            setEndpointIfSupported(clientConfig, stripScheme(config.endpoint()));
        }

        COSClient client = new COSClient(credentials, clientConfig);
        return new TencentCosStorageClient(client);
    }

    private static String stripScheme(String endpoint) {
        if (endpoint.startsWith("http://")) {
            return endpoint.substring("http://".length());
        }
        if (endpoint.startsWith("https://")) {
            return endpoint.substring("https://".length());
        }
        return endpoint;
    }

    private static void setEndpointIfSupported(ClientConfig clientConfig, String endpoint) {
        try {
            java.lang.reflect.Method method = ClientConfig.class.getMethod("setEndpointSuffix", String.class);
            method.invoke(clientConfig, endpoint);
        } catch (ReflectiveOperationException ignored) {
            // Endpoint customization isn't available in this SDK version.
        }
    }
}
