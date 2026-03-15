package io.vacivor.storacle.example;

import io.vacivor.storacle.StorageException;
import io.vacivor.storacle.StoragePolicyViolationException;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ExampleExceptionHandlerTest {
    private final ExampleExceptionHandler handler = new ExampleExceptionHandler();

    @Test
    void mapsPolicyViolationToBadRequest() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/objects");

        ResponseEntity<ApiErrorResponse> response = handler.handleBadRequest(
                new StoragePolicyViolationException("CONTENT_TYPE_NOT_ALLOWED", "Content type not allowed: text/plain"),
                request
        );

        assertEquals(400, response.getStatusCode().value());
        assertEquals("Bad Request", response.getBody().error());
        assertEquals("CONTENT_TYPE_NOT_ALLOWED", response.getBody().errorCode());
        assertEquals("Content type not allowed: text/plain", response.getBody().message());
        assertEquals("/api/objects", response.getBody().path());
    }

    @Test
    void mapsGenericStorageExceptionToServerError() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/objects");

        ResponseEntity<ApiErrorResponse> response = handler.handleStorageFailure(
                new StorageException("Failed to read upload content"),
                request
        );

        assertEquals(500, response.getStatusCode().value());
        assertEquals("Internal Server Error", response.getBody().error());
        assertEquals("INTERNAL_ERROR", response.getBody().errorCode());
        assertEquals("Failed to read upload content", response.getBody().message());
    }
}
