package io.vacivor.storacle.vendor;

import io.vacivor.storacle.ObjectStorageClient;
import io.vacivor.storacle.StorageVendorConfig;

import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

public final class ObjectStorageClientFactoryRegistry {
    private final Map<StorageVendor, ObjectStorageClientFactory> factoriesByVendor;

    public ObjectStorageClientFactoryRegistry(Collection<? extends ObjectStorageClientFactory> factories) {
        Objects.requireNonNull(factories, "factories");
        Map<StorageVendor, ObjectStorageClientFactory> resolvedFactories = new EnumMap<>(StorageVendor.class);
        for (ObjectStorageClientFactory factory : factories) {
            if (factory == null) {
                continue;
            }
            StorageVendor vendor = factory.vendor();
            if (resolvedFactories.containsKey(vendor)) {
                throw new IllegalArgumentException("Duplicate factory for vendor: " + vendor.id());
            }
            resolvedFactories.put(vendor, factory);
        }
        this.factoriesByVendor = Map.copyOf(resolvedFactories);
    }

    public static ObjectStorageClientFactoryRegistry load() {
        return load(Thread.currentThread().getContextClassLoader());
    }

    public static ObjectStorageClientFactoryRegistry load(ClassLoader classLoader) {
        Objects.requireNonNull(classLoader, "classLoader must not be null");
        ServiceLoader<ObjectStorageClientFactory> loader = ServiceLoader.load(ObjectStorageClientFactory.class, classLoader);
        return new ObjectStorageClientFactoryRegistry(loader.stream()
                .map(ServiceLoader.Provider::get)
                .toList());
    }

    public ObjectStorageClient create(StorageVendorConfig config) {
        Objects.requireNonNull(config, "config must not be null");
        StorageVendor vendor = config.vendor();
        ObjectStorageClientFactory factory = factoriesByVendor.get(vendor);
        if (factory == null) {
            throw new IllegalArgumentException("No factory registered for vendor: " + vendor.id());
        }
        return factory.create(config);
    }
}
