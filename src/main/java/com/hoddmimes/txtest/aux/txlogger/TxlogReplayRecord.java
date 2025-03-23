package com.hoddmimes.txtest.aux.txlogger;

public class TxlogReplayRecord
{
    private String mFilename;
    private long mMsgSeqno;
    private byte[] mData;

    public TxlogReplayRecord( byte[] pData, String pFilename, long pMsgSeqno ) {
        mFilename = pFilename;
        mData = pData;
        mMsgSeqno = pMsgSeqno;
    }


    public String getFilename() {
        return mFilename;
    }

    public long getMsgSeqno() {
        return mMsgSeqno;
    }

    public byte[] getData() {
        return mData;
    }

    public boolean isIgnored() {
        return (mMsgSeqno == 0);
    }

    public String toString() {
        return "TxlogReplayRecord[" + mFilename + " msgseqno: " + mMsgSeqno + " datasize: " + mData.length + "]";
    }
}
