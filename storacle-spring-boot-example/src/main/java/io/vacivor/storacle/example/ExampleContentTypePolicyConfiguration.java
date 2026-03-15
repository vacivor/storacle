package io.vacivor.storacle.example;

import io.vacivor.storacle.StoragePolicyViolationException;
import io.vacivor.storacle.template.AllowlistContentTypePolicy;
import io.vacivor.storacle.template.CompositeContentTypePolicy;
import io.vacivor.storacle.template.ContentTypePolicy;
import io.vacivor.storacle.template.ContentTypePolicyResolver;
import io.vacivor.storacle.template.MaxContentLengthPolicy;
import io.vacivor.storacle.template.NoopContentTypePolicy;
import io.vacivor.storacle.template.RoutingContentTypePolicyResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Configuration
public class ExampleContentTypePolicyConfiguration {
    @Bean
    public ContentTypePolicyResolver contentTypePolicyResolver() {
        ContentTypePolicy avatarPolicy = new CompositeContentTypePolicy(List.of(
                new AllowlistContentTypePolicy(Set.of("image/png", "image/jpeg")),
                new MaxContentLengthPolicy(2 * 1024 * 1024)
        ));

        ContentTypePolicy documentPolicy = new CompositeContentTypePolicy(List.of(
                new AllowlistContentTypePolicy(Set.of("application/pdf")),
                new MaxContentLengthPolicy(10 * 1024 * 1024)
        ));

        ContentTypePolicy defaultPolicy = context -> {
            if ("application/x-msdownload".equalsIgnoreCase(context.contentType())) {
                throw new StoragePolicyViolationException(
                        "CONTENT_TYPE_NOT_ALLOWED",
                        "Content type not allowed: " + context.contentType()
                );
            }
        };

        return new RoutingContentTypePolicyResolver(
                Map.of(
                        "avatar", avatarPolicy,
                        "document", documentPolicy
                ),
                new CompositeContentTypePolicy(List.of(defaultPolicy, new NoopContentTypePolicy()))
        );
    }
}
