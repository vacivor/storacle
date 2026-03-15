package io.vacivor.storacle.spring.autoconfigure;

import io.vacivor.storacle.ContentTypeDetector;
import io.vacivor.storacle.DefaultContentTypeDetector;
import io.vacivor.storacle.FilenameGenerator;
import io.vacivor.storacle.ObjectStorageClient;
import io.vacivor.storacle.TikaContentTypeDetector;
import io.vacivor.storacle.UuidFilenameGenerator;
import io.vacivor.storacle.vendor.ObjectStorageClientFactoryRegistry;
import io.vacivor.storacle.StorageVendorConfig;
import io.vacivor.storacle.template.ContentTypePolicyResolver;
import io.vacivor.storacle.template.DefaultContentTypePolicyResolver;
import io.vacivor.storacle.template.ObjectStorageTemplate;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@EnableConfigurationProperties(StoracleProperties.class)
public class StoracleAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    public ObjectStorageClientFactoryRegistry objectStorageClientFactoryRegistry() {
        return ObjectStorageClientFactoryRegistry.load();
    }

    @Bean
    @ConditionalOnMissingBean
    public ObjectStorageClient objectStorageClient(StoracleProperties properties,
                                                   ObjectStorageClientFactoryRegistry factoryRegistry) {
        StorageVendorConfig config = properties.resolveConfig();
        return factoryRegistry.create(config);
    }

    @Bean
    @ConditionalOnMissingBean
    public ContentTypeDetector contentTypeDetector(StoracleProperties properties) {
        return switch (properties.getContentTypeDetector()) {
            case TIKA -> new TikaContentTypeDetector();
            case DEFAULT -> new DefaultContentTypeDetector();
        };
    }

    @Bean
    @ConditionalOnMissingBean
    public FilenameGenerator filenameGenerator() {
        return new UuidFilenameGenerator();
    }

    @Bean
    @ConditionalOnMissingBean
    public ContentTypePolicyResolver contentTypePolicyResolver() {
        return new DefaultContentTypePolicyResolver();
    }

    @Bean
    @ConditionalOnMissingBean
    public ObjectStorageTemplate objectStorageTemplate(ObjectStorageClient client,
                                                       ContentTypeDetector detector,
                                                       FilenameGenerator generator,
                                                       ContentTypePolicyResolver contentTypePolicyResolver) {
        return new ObjectStorageTemplate(client, detector, generator, contentTypePolicyResolver);
    }
}
