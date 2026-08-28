package com.cloudrelay.exception;

import lombok.Getter;

@Getter
public class SessionFullException extends RuntimeException {

    private final String sessionCode;
    private final int maxPlayers;

    public SessionFullException(String sessionCode, int maxPlayers) {
        super("Session " + sessionCode + " is full with " + maxPlayers + " players");
        this.sessionCode = sessionCode;
        this.maxPlayers = maxPlayers;
    }
}
