package io.vacivor.storacle;

public record ObjectWriteResult(ObjectPath path, String eTag, String versionId) {
}
