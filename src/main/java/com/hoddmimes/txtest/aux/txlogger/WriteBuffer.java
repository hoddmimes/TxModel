package com.hoddmimes.txtest.aux.txlogger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class WriteBuffer
{
    private static byte[] cFiller = new byte[10240];
    static {
        Arrays.fill(cFiller, (byte) 'z');
    }

    private ByteBuffer  mBuffer;
    private boolean     mTempBuffer;
    private int         mAlignSize;
    private List<Callback> mCallbacks;


    public WriteBuffer(int pBufferSize, int pAlignSize, boolean pTempBuffer)
    {
        mAlignSize = pAlignSize;
        mBuffer = ByteBuffer.allocateDirect( pBufferSize +  pAlignSize);
        mTempBuffer = pTempBuffer;
        mCallbacks = null;
    }

    public void clear() {
        mBuffer.clear();
    }

    public boolean isTempBuffer() {
        return mTempBuffer;
    }

    public void put( byte[] pMessage, long pSequenceNo ) {
        this.put( pMessage,  pSequenceNo, null, null);
    }

    public void put( byte[] pMessage, long pSequenceNo, TxlogWriteCallback pCallback, Object pCallbackParameter ) {
        mBuffer.putInt( pMessage.length);
        mBuffer.putLong( pSequenceNo );
        mBuffer.put( pMessage );
        mBuffer.putInt( pMessage.length);
        mBuffer.putInt( TxLogger.MAGIC_END);
        if (pCallback != null) {
            if (this.mCallbacks == null) {
                this.mCallbacks = new ArrayList<>();
            }
            this.mCallbacks.add( new Callback( pCallback, pCallbackParameter));
        }
    }

    public boolean doesMsgFit( byte[] pMsg) {
        if (mBuffer.remaining() - (mAlignSize + TxLogger.RECORD_HEADER_SIZE) - (+ pMsg.length + TxLogger.RECORD_HEADER_SIZE) >= 0) {
            return true;
        }
        return false;
    }

    public void executeCallbacks() {
        if (mCallbacks == null) {
            return;
        }

        for (Callback c : mCallbacks) {
            try {
                c.mCallback.txlogWriteComplete(c.mCallbackParameter);
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    public int remaining() {
        return mBuffer.remaining();
    }

    public long getPosition() {
        return mBuffer.position();
    }
    public void flip() {
        mBuffer.flip();
    }

    public ByteBuffer getBuffer(){
        return mBuffer;
    }

    public int getInt()
    {
        return mBuffer.getInt();
    }

    public void align() {
        int tFillerSize = (int) mAlignSize - (mBuffer.position() % mAlignSize);
        if ((tFillerSize == 0) || (tFillerSize == mAlignSize)) {
            return;
        }

        if (tFillerSize < TxLogger.RECORD_HEADER_SIZE) {
            tFillerSize += mAlignSize;
        }

        mBuffer.putInt(tFillerSize);
        mBuffer.putLong(0);
        mBuffer.put(cFiller, 0, tFillerSize );
        mBuffer.putInt(tFillerSize);
        mBuffer.putInt(TxLogger.MAGIC_END);
    }

    public void getBytes( byte[] msg)
    {
        mBuffer.get( msg );
    }

    public byte[] getBytes( int size ) {
        byte[] tMsg = new byte[size];
        mBuffer.get( tMsg );
        return tMsg;
    }

    class Callback {
        TxlogWriteCallback mCallback;
        Object mCallbackParameter;

        Callback( TxlogWriteCallback pCallback, Object pCallbackParameter ) {
            mCallback = pCallback;
            mCallbackParameter = pCallbackParameter;
        }
    }

}
