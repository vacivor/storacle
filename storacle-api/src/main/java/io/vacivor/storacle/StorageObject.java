package io.vacivor.storacle;

import java.util.Objects;


import java.io.IOException;
import java.io.InputStream;

public final class StorageObject implements AutoCloseable {
    private final ObjectPath path;
    private final ObjectMetadata metadata;
    private final InputStream content;

    public StorageObject(ObjectPath path, ObjectMetadata metadata, InputStream content) {
        this.path = Objects.requireNonNull(path, "path must not be null");
        this.metadata = Objects.requireNonNull(metadata, "metadata must not be null");
        this.content = Objects.requireNonNull(content, "content must not be null");
    }

    public ObjectPath path() {
        return path;
    }

    public ObjectMetadata metadata() {
        return metadata;
    }

    public InputStream content() {
        return content;
    }

    @Override
    public void close() throws IOException {
        content.close();
    }
}
