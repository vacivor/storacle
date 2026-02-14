package io.vacivor.storacle.client;

import java.util.Objects;

import com.qiniu.storage.DownloadUrl;
import com.qiniu.common.QiniuException;
import com.qiniu.util.Auth;
import io.vacivor.storacle.StorageVendorOptions;
import io.vacivor.storacle.StorageException;

public final class DefaultQiniuDownloadUrlProvider implements QiniuDownloadUrlProvider {
    private final Auth auth;
    private final StorageVendorOptions options;

    public DefaultQiniuDownloadUrlProvider(Auth auth, StorageVendorOptions options) {
        this.auth = Objects.requireNonNull(auth, "auth must not be null");
        this.options = Objects.requireNonNull(options, "options must not be null");
    }

    @Override
    public String resolve(String bucket, String key) {
        String domain = options.get("downloadDomain");
        if (domain == null || domain.isBlank()) {
            throw new StorageException("Qiniu downloadDomain option is required for get() operations");
        }
        boolean https = options.getBoolean("https", true);
        long expires = options.getLong("downloadExpires", 3600L);
        boolean privateBucket = options.getBoolean("privateBucket", true);

        DownloadUrl downloadUrl = new DownloadUrl(domain, https, key);
        try {
            return privateBucket ? downloadUrl.buildURL(auth, expires) : downloadUrl.buildURL();
        } catch (QiniuException e) {
            throw new StorageException("Failed to build Qiniu download URL", e);
        }
    }
}
