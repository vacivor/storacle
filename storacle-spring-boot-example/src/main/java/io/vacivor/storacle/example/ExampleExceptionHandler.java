package io.vacivor.storacle.example;

import io.vacivor.storacle.StorageException;
import io.vacivor.storacle.StoragePolicyViolationException;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;

@RestControllerAdvice
public class ExampleExceptionHandler {
    @ExceptionHandler({StoragePolicyViolationException.class, IllegalArgumentException.class})
    public ResponseEntity<ApiErrorResponse> handleBadRequest(RuntimeException exception,
                                                             HttpServletRequest request) {
        return build(HttpStatus.BAD_REQUEST, exception, request);
    }

    @ExceptionHandler(StorageException.class)
    public ResponseEntity<ApiErrorResponse> handleStorageFailure(StorageException exception,
                                                                 HttpServletRequest request) {
        return build(HttpStatus.INTERNAL_SERVER_ERROR, exception, request);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiErrorResponse> handleServerError(Exception exception,
                                                              HttpServletRequest request) {
        return build(HttpStatus.INTERNAL_SERVER_ERROR, exception, request);
    }

    private static ResponseEntity<ApiErrorResponse> build(HttpStatus status, Exception exception,
                                                          HttpServletRequest request) {
        String errorCode = exception instanceof StoragePolicyViolationException violationException
                ? violationException.errorCode()
                : "INTERNAL_ERROR";
        ApiErrorResponse body = new ApiErrorResponse(
                Instant.now(),
                status.value(),
                status.getReasonPhrase(),
                errorCode,
                exception.getMessage(),
                request.getRequestURI()
        );
        return ResponseEntity.status(status).body(body);
    }
}
