package io.vacivor.storacle.template;

import io.vacivor.storacle.StoragePolicyViolationException;

import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

public final class AllowlistContentTypePolicy implements ContentTypePolicy {
    private final Set<String> allowedContentTypes;

    public AllowlistContentTypePolicy(Set<String> allowedContentTypes) {
        Objects.requireNonNull(allowedContentTypes, "allowedContentTypes must not be null");
        this.allowedContentTypes = normalize(allowedContentTypes);
        if (this.allowedContentTypes.isEmpty()) {
            throw new IllegalArgumentException("allowedContentTypes must not be empty");
        }
    }

    @Override
    public void validate(UploadContext context) {
        Objects.requireNonNull(context, "context must not be null");
        String contentType = normalize(context.contentType());
        if (contentType == null || !allowedContentTypes.contains(contentType)) {
            throw new StoragePolicyViolationException(
                    "CONTENT_TYPE_NOT_ALLOWED",
                    "Content type not allowed: " + context.contentType()
            );
        }
    }

    public Set<String> allowedContentTypes() {
        return allowedContentTypes;
    }

    private static Set<String> normalize(Set<String> contentTypes) {
        Set<String> normalized = new LinkedHashSet<>();
        for (String contentType : contentTypes) {
            String value = normalize(contentType);
            if (value != null) {
                normalized.add(value);
            }
        }
        return Set.copyOf(normalized);
    }

    private static String normalize(String contentType) {
        if (contentType == null) {
            return null;
        }
        String normalized = contentType.trim().toLowerCase(Locale.ROOT);
        return normalized.isEmpty() ? null : normalized;
    }
}
