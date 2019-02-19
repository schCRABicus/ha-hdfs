package com.epam.bigdata.training.app.hotels;

/**
 * Exception occurred while analyzing hotels dataset.
 */
public class HotelsAnalyzingException extends RuntimeException {

    public HotelsAnalyzingException(String message) {
        super(message);
    }

    public HotelsAnalyzingException(String message, Throwable cause) {
        super(message, cause);
    }
}
