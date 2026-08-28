package com.cloudrelay.model;

/**
 * Lifecycle states for a cloud gaming session.
 * WAITING means the session was created and is waiting for players.
 * STARTING means the minimum player count was reached and the session is launching.
 * ACTIVE means the game is running.
 * PAUSED means the game is paused during a player disconnect grace period.
 * TERMINATED means the session has ended.
 */
public enum SessionState {
    WAITING,
    STARTING,
    ACTIVE,
    PAUSED,
    TERMINATED
}
