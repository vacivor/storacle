package io.vacivor.storacle;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ObjectMetadataMapperTest {
    @Test
    void applyCopiesAllSupportedFields() {
        ObjectMetadata metadata = ObjectMetadata.builder()
                .contentType("image/png")
                .contentLength(42L)
                .cacheControl("max-age=60")
                .contentDisposition("inline")
                .userMetadata(Map.of("owner", "storacle"))
                .build();
        Map<String, Object> target = new HashMap<>();

        ObjectMetadataMapper.apply(
                metadata,
                value -> target.put("contentType", value),
                value -> target.put("contentLength", value),
                value -> target.put("cacheControl", value),
                value -> target.put("contentDisposition", value),
                value -> target.put("userMetadata", value)
        );

        assertEquals("image/png", target.get("contentType"));
        assertEquals(42L, target.get("contentLength"));
        assertEquals("max-age=60", target.get("cacheControl"));
        assertEquals("inline", target.get("contentDisposition"));
        assertEquals(Map.of("owner", "storacle"), target.get("userMetadata"));
    }

    @Test
    void fromBuildsObjectMetadataFromSuppliers() {
        ObjectMetadata metadata = ObjectMetadataMapper.from(
                () -> "application/json",
                () -> 7L,
                () -> "no-cache",
                () -> "attachment",
                () -> Map.of("source", "test")
        );

        assertEquals("application/json", metadata.contentType());
        assertEquals(7L, metadata.contentLength());
        assertEquals("no-cache", metadata.cacheControl());
        assertEquals("attachment", metadata.contentDisposition());
        assertEquals("test", metadata.userMetadata().get("source"));
    }
}
