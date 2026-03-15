package io.vacivor.storacle.template;

import java.util.Objects;

public final class DefaultContentTypePolicyResolver implements ContentTypePolicyResolver {
    private final ContentTypePolicy defaultPolicy;

    public DefaultContentTypePolicyResolver() {
        this(new NoopContentTypePolicy());
    }

    public DefaultContentTypePolicyResolver(ContentTypePolicy defaultPolicy) {
        this.defaultPolicy = Objects.requireNonNull(defaultPolicy, "defaultPolicy must not be null");
    }

    @Override
    public ContentTypePolicy resolve(UploadContext context) {
        return defaultPolicy;
    }
}
