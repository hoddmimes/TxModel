package com.hoddmimes.txtest.server;


public enum ServerRole {
    UNKNOWN(0), PRIMARY(1), STANDBY(2);

    private int mValue;

    ServerRole(int numVal) {
        this.mValue = numVal;
    }

    public int getValue() {
        return mValue;
    }

    public static ServerRole valueOf(int pRole) {
        if (pRole == UNKNOWN.getValue()) {
            return UNKNOWN;
        } else if (pRole == PRIMARY.getValue()) {
            return PRIMARY;
        } else if (pRole == STANDBY.getValue()) {
            return STANDBY;
        }
        return null;
    }

}
