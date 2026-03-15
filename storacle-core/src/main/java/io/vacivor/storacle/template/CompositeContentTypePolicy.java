package io.vacivor.storacle.template;

import java.util.List;
import java.util.Objects;

public final class CompositeContentTypePolicy implements ContentTypePolicy {
    private final List<ContentTypePolicy> policies;

    public CompositeContentTypePolicy(List<ContentTypePolicy> policies) {
        Objects.requireNonNull(policies, "policies must not be null");
        if (policies.isEmpty()) {
            throw new IllegalArgumentException("policies must not be empty");
        }
        this.policies = List.copyOf(policies);
        for (ContentTypePolicy policy : this.policies) {
            Objects.requireNonNull(policy, "policies must not contain null");
        }
    }

    @Override
    public void validate(UploadContext context) {
        for (ContentTypePolicy policy : policies) {
            policy.validate(context);
        }
    }

    public List<ContentTypePolicy> policies() {
        return policies;
    }
}
