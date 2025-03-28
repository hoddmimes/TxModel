package com.hoddmimes.txtest.aux.txlogger;

public class TxlogReplyEntryMessage extends TxlogEntry implements TxlogReplyEntryInterface
{
    private String mFilename;

    TxlogReplyEntryMessage(byte[] pPayload, String pFilename)  {
        super(pPayload);
        mFilename = pFilename;
    }
    public String getFilename() {
        return mFilename;
    }

    @Override
    public int getType() {
        return WriteBuffer.FLAG_USER_PAYLOAD;
    }
}
