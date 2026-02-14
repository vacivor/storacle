package io.vacivor.storacle;

import java.time.Instant;

public record ObjectSummary(ObjectPath path, long size, Instant lastModified, String eTag) {
}
