package com.hoddmimes.txtest.server;


public enum ServerState {
    UNKNOWN(0), PRIMARY(1), STANDBY(2);

    private int mValue;

    ServerState(int numVal) {
        this.mValue = numVal;
    }

    public int getValue() {
        return mValue;
    }

    public static ServerState valueOf( int pStateValue) {
        if (pStateValue == UNKNOWN.getValue()) {
            return UNKNOWN;
        } else if (pStateValue == PRIMARY.getValue()) {
            return PRIMARY;
        } else if (pStateValue == STANDBY.getValue()) {
            return STANDBY;
        }
        return null;
    }

}
