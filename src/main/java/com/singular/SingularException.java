package com.singular;

/**
 * Runtime exception for communicating yarn related errors.
 *
 * @author Rahul Bhattacharjee
 */
public class SingularException extends RuntimeException {
    public SingularException() {
    }

    public SingularException(String message) {
        super(message);
    }

    public SingularException(String message, Throwable cause) {
        super(message, cause);
    }
}
