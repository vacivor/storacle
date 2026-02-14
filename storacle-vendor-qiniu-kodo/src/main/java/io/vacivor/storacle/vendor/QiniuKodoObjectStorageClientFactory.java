package io.vacivor.storacle.vendor;

import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.UploadManager;
import com.qiniu.util.Auth;
import io.vacivor.storacle.ObjectStorageClient;
import io.vacivor.storacle.StorageVendorConfig;
import io.vacivor.storacle.client.QiniuKodoStorageClient;
import io.vacivor.storacle.client.QiniuDownloadUrlProvider;
import io.vacivor.storacle.client.DefaultQiniuDownloadUrlProvider;

public final class QiniuKodoObjectStorageClientFactory extends AbstractObjectStorageClientFactory {
    public QiniuKodoObjectStorageClientFactory() {
        super(StorageVendor.QINIU_KODO);
    }

    @Override
    public ObjectStorageClient create(StorageVendorConfig config) {
        assertVendor(config);

        String accessKey = requireNonBlank(config.accessKey(), "accessKey");
        String secretKey = requireNonBlank(config.secretKey(), "secretKey");
        String region = config.region();

        Configuration cfg = QiniuKodoStorageClient.resolveConfiguration(region);
        Auth auth = Auth.create(accessKey, secretKey);
        UploadManager uploadManager = new UploadManager(cfg);
        BucketManager bucketManager = new BucketManager(auth, cfg);

        QiniuDownloadUrlProvider downloadUrlProvider = new DefaultQiniuDownloadUrlProvider(auth, config.options());
        return new QiniuKodoStorageClient(auth, uploadManager, bucketManager, downloadUrlProvider);
    }

    private static String requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }
}
