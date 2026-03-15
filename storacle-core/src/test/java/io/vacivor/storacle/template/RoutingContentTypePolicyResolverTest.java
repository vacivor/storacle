package io.vacivor.storacle.template;

import io.vacivor.storacle.ObjectMetadata;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertSame;

class RoutingContentTypePolicyResolverTest {
    @Test
    void resolvesPolicyByScene() {
        ContentTypePolicy avatarPolicy = context -> {
        };
        ContentTypePolicy defaultPolicy = context -> {
        };
        RoutingContentTypePolicyResolver resolver = new RoutingContentTypePolicyResolver(
                Map.of("avatar", avatarPolicy),
                defaultPolicy
        );

        ContentTypePolicy resolved = resolver.resolve(context("avatar"));

        assertSame(avatarPolicy, resolved);
    }

    @Test
    void fallsBackToDefaultPolicyWhenSceneMissing() {
        ContentTypePolicy defaultPolicy = context -> {
        };
        RoutingContentTypePolicyResolver resolver = new RoutingContentTypePolicyResolver(Map.of(), defaultPolicy);

        ContentTypePolicy resolved = resolver.resolve(context(null));

        assertSame(defaultPolicy, resolved);
    }

    @Test
    void trimsSceneBeforeLookup() {
        AtomicInteger calls = new AtomicInteger();
        ContentTypePolicy avatarPolicy = context -> calls.incrementAndGet();
        RoutingContentTypePolicyResolver resolver = new RoutingContentTypePolicyResolver(Map.of("avatar", avatarPolicy));

        resolver.resolve(context(" avatar ")).validate(context(" avatar "));

        org.junit.jupiter.api.Assertions.assertEquals(1, calls.get());
    }

    private static UploadContext context(String scene) {
        return new UploadContext(
                scene,
                "bucket",
                "key",
                "file.txt",
                "text/plain",
                5L,
                ObjectMetadata.empty()
        );
    }
}
