package com.hoddmimes.txtest.aux.txlogger;

public class TxlogReplayRecord
{
    private String mFilename;
    private long mReplayOption;
    private byte[] mData;

    public TxlogReplayRecord( byte[] pData, String pFilename, int pReplayOption ) {
        mFilename = pFilename;
        mData = pData;
        mReplayOption = pReplayOption;
    }


    public String getFilename() {
        return mFilename;
    }

    public long getmReplayOption() {
        return mReplayOption;
    }

    public byte[] getData() {
        return mData;
    }

    public boolean isIgnored() {
        return (mReplayOption == TxLogger.REPLAY_OPTION_IGNORE);
    }
}
