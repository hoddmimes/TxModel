package com.hoddmimes.txtest.aux.txlogger;

public class TxlogEntry
{
    private long mMessageSeqno;
    private long mTxid;
    private byte[] mMsgPayload;

    public TxlogEntry( byte[] pPayload, long pMsgSeqno, long pTxid) {
        mMessageSeqno = pMsgSeqno;
        mTxid = pTxid;
        mMsgPayload = pPayload;
    }

    public long getMessageSeqno() {
        return mMessageSeqno;
    }

    public long getTxid() {
        return mTxid;
    }

    public byte[] getMsgPayload() {
        return mMsgPayload;
    }

    public TxlogEntry( byte[] pData) {
        mMessageSeqno = TxlogAux.bytesToLong(pData, 0);
        mTxid = TxlogAux.bytesToLong(pData, Long.BYTES);
        mMsgPayload = new byte[ pData.length - (2 * Long.BYTES)];
        System.arraycopy(pData, (2 * Long.BYTES), mMsgPayload, 0, pData.length - (2 * Long.BYTES));
    }

    public byte[] toBytes() {
        byte[] tData = new byte[ (2 * Long.BYTES) + mMsgPayload.length];
        TxlogAux.longToBytes(mMessageSeqno, tData, 0);
        TxlogAux.longToBytes(mTxid, tData, Long.BYTES);
        System.arraycopy(mMsgPayload, 0, tData, (2* Long.BYTES), mMsgPayload.length);
        return tData;
    }





}
