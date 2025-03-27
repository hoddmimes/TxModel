package com.hoddmimes.txtest.aux.txlogger;

public class TxlogReplyEntryFiller implements TxlogReplyEntryInterface
{
    private int mFillerSize;


    TxlogReplyEntryFiller( int pFillerSize) {
        mFillerSize = pFillerSize;
    }

    @Override
    public int getType() {
        return WriteBuffer.FLAG_PADDING_PAYLOAD;
    }

    public int getFillerSize() {
        return mFillerSize;
    }
}
