package io.vacivor.storacle.spring.autoconfigure;

import io.vacivor.storacle.ContentTypeDetector;
import io.vacivor.storacle.DefaultContentTypeDetector;
import io.vacivor.storacle.FilenameGenerator;
import io.vacivor.storacle.ObjectStorageClient;
import io.vacivor.storacle.vendor.ObjectStorageClientFactory;
import io.vacivor.storacle.UuidFilenameGenerator;
import io.vacivor.storacle.vendor.ObjectStorageClientFactoryRegistry;
import io.vacivor.storacle.StorageVendorConfig;
import io.vacivor.storacle.template.ObjectStorageTemplate;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.List;
import java.util.ServiceLoader;

@AutoConfiguration
@EnableConfigurationProperties(StoracleProperties.class)
public class StoracleAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    public ObjectStorageClientFactoryRegistry objectStorageClientFactoryRegistry() {
        List<ObjectStorageClientFactory> factories = ServiceLoader.load(ObjectStorageClientFactory.class)
                .stream()
                .map(ServiceLoader.Provider::get)
                .toList();
        return new ObjectStorageClientFactoryRegistry(factories);
    }

    @Bean
    @ConditionalOnMissingBean
    public ObjectStorageClient objectStorageClient(StoracleProperties properties,
                                                   ObjectStorageClientFactoryRegistry factoryRegistry) {
        StorageVendorConfig config = properties.resolveConfig();
        return factoryRegistry.create(properties.resolveVendor(), config);
    }

    @Bean
    @ConditionalOnMissingBean
    public ContentTypeDetector contentTypeDetector() {
        return new DefaultContentTypeDetector();
    }

    @Bean
    @ConditionalOnMissingBean
    public FilenameGenerator filenameGenerator() {
        return new UuidFilenameGenerator();
    }

    @Bean
    @ConditionalOnMissingBean
    public ObjectStorageTemplate objectStorageTemplate(ObjectStorageClient client,
                                                       ContentTypeDetector detector,
                                                       FilenameGenerator generator) {
        return new ObjectStorageTemplate(client, detector, generator);
    }
}
