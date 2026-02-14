package io.vacivor.storacle;

import java.util.Collections;
import java.util.List;

public final class ListObjectsPage {
    private final List<ObjectSummary> objects;
    private final String nextContinuationToken;
    private final boolean truncated;

    public ListObjectsPage(List<ObjectSummary> objects, String nextContinuationToken, boolean truncated) {
        this.objects = objects == null ? List.of() : Collections.unmodifiableList(objects);
        this.nextContinuationToken = nextContinuationToken;
        this.truncated = truncated;
    }

    public List<ObjectSummary> objects() {
        return objects;
    }

    public String nextContinuationToken() {
        return nextContinuationToken;
    }

    public boolean truncated() {
        return truncated;
    }
}
