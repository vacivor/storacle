package io.vacivor.storacle.template;

public final class NoopContentTypePolicy implements ContentTypePolicy {
    @Override
    public void validate(UploadContext context) {
        // Intentionally empty.
    }
}
