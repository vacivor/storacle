package io.vacivor.storacle.template;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class RoutingContentTypePolicyResolver implements ContentTypePolicyResolver {
    private final Map<String, ContentTypePolicy> scenePolicies;
    private final ContentTypePolicy defaultPolicy;

    public RoutingContentTypePolicyResolver(Map<String, ContentTypePolicy> scenePolicies) {
        this(scenePolicies, new NoopContentTypePolicy());
    }

    public RoutingContentTypePolicyResolver(Map<String, ContentTypePolicy> scenePolicies,
                                            ContentTypePolicy defaultPolicy) {
        Objects.requireNonNull(scenePolicies, "scenePolicies must not be null");
        this.defaultPolicy = Objects.requireNonNull(defaultPolicy, "defaultPolicy must not be null");
        this.scenePolicies = normalize(scenePolicies);
    }

    @Override
    public ContentTypePolicy resolve(UploadContext context) {
        Objects.requireNonNull(context, "context must not be null");
        String scene = normalizeScene(context.scene());
        if (scene == null) {
            return defaultPolicy;
        }
        return scenePolicies.getOrDefault(scene, defaultPolicy);
    }

    private static Map<String, ContentTypePolicy> normalize(Map<String, ContentTypePolicy> scenePolicies) {
        Map<String, ContentTypePolicy> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, ContentTypePolicy> entry : scenePolicies.entrySet()) {
            String scene = normalizeScene(entry.getKey());
            ContentTypePolicy policy = Objects.requireNonNull(entry.getValue(), "scene policy must not be null");
            if (scene != null) {
                normalized.put(scene, policy);
            }
        }
        return Map.copyOf(normalized);
    }

    private static String normalizeScene(String scene) {
        if (scene == null) {
            return null;
        }
        String normalized = scene.trim();
        return normalized.isEmpty() ? null : normalized;
    }
}
