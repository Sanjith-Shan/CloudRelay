package com.cloudrelay.exception;

import lombok.Getter;

@Getter
public class SessionNotFoundException extends RuntimeException {

    private final String sessionCode;

    public SessionNotFoundException(String sessionCode) {
        super("Session not found with code " + sessionCode);
        this.sessionCode = sessionCode;
    }
}
