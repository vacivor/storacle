package io.vacivor.storacle.template;

public interface ContentTypePolicyResolver {
    ContentTypePolicy resolve(UploadContext context);
}
