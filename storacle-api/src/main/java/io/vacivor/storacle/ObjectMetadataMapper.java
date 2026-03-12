package io.vacivor.storacle;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class ObjectMetadataMapper {
    private ObjectMetadataMapper() {
    }

    public static void apply(
            ObjectMetadata metadata,
            Consumer<String> contentTypeSetter,
            Consumer<Long> contentLengthSetter,
            Consumer<String> cacheControlSetter,
            Consumer<String> contentDispositionSetter,
            Consumer<Map<String, String>> userMetadataSetter
    ) {
        if (metadata.contentType() != null) {
            contentTypeSetter.accept(metadata.contentType());
        }
        if (metadata.contentLength() != null && metadata.contentLength() >= 0) {
            contentLengthSetter.accept(metadata.contentLength());
        }
        if (metadata.cacheControl() != null) {
            cacheControlSetter.accept(metadata.cacheControl());
        }
        if (metadata.contentDisposition() != null) {
            contentDispositionSetter.accept(metadata.contentDisposition());
        }
        if (!metadata.userMetadata().isEmpty()) {
            userMetadataSetter.accept(metadata.userMetadata());
        }
    }

    public static ObjectMetadata from(
            Supplier<String> contentTypeSupplier,
            Supplier<Long> contentLengthSupplier,
            Supplier<String> cacheControlSupplier,
            Supplier<String> contentDispositionSupplier,
            Supplier<Map<String, String>> userMetadataSupplier
    ) {
        return ObjectMetadata.builder()
                .contentType(contentTypeSupplier.get())
                .contentLength(contentLengthSupplier.get())
                .cacheControl(cacheControlSupplier.get())
                .contentDisposition(contentDispositionSupplier.get())
                .userMetadata(userMetadataSupplier.get())
                .build();
    }
}
