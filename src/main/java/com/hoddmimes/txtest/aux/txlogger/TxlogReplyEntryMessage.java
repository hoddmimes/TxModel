package com.hoddmimes.txtest.aux.txlogger;

public class TxlogReplyEntryMessage implements TxlogReplyEntryInterface
{
    private long mMsgSeqno;
    private byte[] mMsgData;
    private String mFilename;

    TxlogReplyEntryMessage(byte[] pPayload, long pMessageSeqno, String pFilename)  {
        mMsgSeqno = pMessageSeqno;
        mMsgData = pPayload;
        mFilename = pFilename;
    }

    @Override
    public int getType() {
        return WriteBuffer.FLAG_USER_PAYLOAD;
    }

    public long getMessageSeqno() {
        return mMsgSeqno;
    }
    public byte[] getMessageData() {
        return mMsgData;
    }
    public String getmFilename() { return mFilename; }
}
