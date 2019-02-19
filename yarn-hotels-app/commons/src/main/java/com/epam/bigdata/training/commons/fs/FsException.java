package com.epam.bigdata.training.commons.fs;

/**
 * Exception thrown during communication with file system.
 */
public class FsException extends RuntimeException {

    public FsException(String message, Throwable cause) {
        super(message, cause);
    }
}
