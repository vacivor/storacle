package io.vacivor.storacle.template;

import io.vacivor.storacle.StoragePolicyViolationException;

public final class MaxContentLengthPolicy implements ContentTypePolicy {
    private final long maxContentLength;

    public MaxContentLengthPolicy(long maxContentLength) {
        if (maxContentLength < 0) {
            throw new IllegalArgumentException("maxContentLength must be >= 0");
        }
        this.maxContentLength = maxContentLength;
    }

    @Override
    public void validate(UploadContext context) {
        Long contentLength = context.contentLength();
        if (contentLength != null && contentLength > maxContentLength) {
            throw new StoragePolicyViolationException(
                    "CONTENT_LENGTH_EXCEEDED",
                    "Content length exceeds limit: " + contentLength + " > " + maxContentLength
            );
        }
    }

    public long maxContentLength() {
        return maxContentLength;
    }
}
