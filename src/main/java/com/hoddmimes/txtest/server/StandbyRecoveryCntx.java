package com.hoddmimes.txtest.server;

import com.hoddmimes.txtest.generated.ipc.messages.ToStandby;

import java.util.ArrayList;
import java.util.List;

public class StandbyRecoveryCntx
{
    public enum ServerState { Recovery, Synchronizing,  };
    private long    mLastKnownSeqnoAtStart;
    private int     mRecoveryMsgsReceived;
    private long    mCurrentSeqno;
    private List<ToStandby> mBufferedMessages;

    StandbyRecoveryCntx( long pLastKnownSeqno ) {
        mLastKnownSeqnoAtStart = pLastKnownSeqno;
    }

    synchronized void addToStandbyMessage( ToStandby pMessagge) {
        mBufferedMessages.add(pMessagge);
    }

    void incrementMessagesReceived() {
        mRecoveryMsgsReceived++;
    }

    void reset() {
        mRecoveryMsgsReceived = 0;
        mLastKnownSeqnoAtStart = this.mCurrentSeqno;
        mBufferedMessages = new ArrayList<ToStandby>();
    }

    List<ToStandby> getBufferedMessages() {
        return mBufferedMessages;
    }

    int getMessagesReceived() {
        return mRecoveryMsgsReceived;
    }

    long getCurrentSeqno() {
        return mCurrentSeqno;
    }

    void setCurrentSeqno(long pCurrentSeqno) {
        mCurrentSeqno = pCurrentSeqno;
    }

    long getLastKnownSeqno() {
        return mLastKnownSeqnoAtStart;
    }

}
