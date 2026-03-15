package io.vacivor.storacle;

public class StoragePolicyViolationException extends StorageException {
    private final String errorCode;

    public StoragePolicyViolationException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public String errorCode() {
        return errorCode;
    }
}
