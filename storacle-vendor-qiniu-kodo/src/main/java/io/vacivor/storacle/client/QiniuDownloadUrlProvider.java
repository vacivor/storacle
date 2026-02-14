package io.vacivor.storacle.client;

public interface QiniuDownloadUrlProvider {
    String resolve(String bucket, String key);
}
