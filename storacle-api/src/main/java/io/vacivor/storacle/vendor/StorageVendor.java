package io.vacivor.storacle.vendor;

import java.util.Locale;

public enum StorageVendor {
    MINIO("minio"),
    TENCENT_COS("tencentcos"),
    QINIU_KODO("qiniukodo"),
    HUAWEI_OBS("huaweiobs"),
    GOOGLE_CLOUD("googlecloud"),
    BAIDU_BOS("baidubos"),
    AMAZON_S3("amazons3"),
    ALIYUN_OSS("aliyunoss");

    private final String id;

    StorageVendor(String id) {
        this.id = id;
    }

    public String id() {
        return id;
    }

    public static StorageVendor fromId(String id) {
        if (id == null) {
            throw new IllegalArgumentException("vendor id must not be null");
        }
        String normalized = id.toLowerCase(Locale.ROOT);
        for (StorageVendor vendor : values()) {
            if (vendor.id.equals(normalized)) {
                return vendor;
            }
        }
        throw new IllegalArgumentException("Unknown vendor id: " + id);
    }
}
