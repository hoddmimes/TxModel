package com.hoddmimes.txtest.server;

import java.nio.ByteBuffer;

public class Asset
{
    private int     mAssetId;
    private int     mValue;
    private long    mUpdateTime;
    private int     mSeqNo;

    public Asset( int pAssetId ) {
        mAssetId = pAssetId;
        mValue = 0;
        mUpdateTime = 0;
        mSeqNo = 0;
    }

    public int getAssetId() {
        return mAssetId;
    }

    public int getValue() {
        return mValue;
    }

    public long getUpdateTime() {
        return mUpdateTime;
    }

    public int getSeqNo() {
        return mSeqNo;
    }

    public void update( int pNewValue ) {
        mValue = pNewValue;
        mUpdateTime = System.currentTimeMillis();
        mSeqNo++;
    }

    public byte[] encode() {
        ByteBuffer tBuffer = ByteBuffer.allocate(16);
        tBuffer.putInt( mAssetId );
        tBuffer.putInt( mValue );
        tBuffer.putLong( mUpdateTime );
        tBuffer.putInt( mSeqNo );
        return tBuffer.array();
    }

    public void decode( byte[] pBuffer ) {
        ByteBuffer tBuffer = ByteBuffer.wrap( pBuffer );
        mAssetId = tBuffer.getInt();
        mValue = tBuffer.getInt();
        mUpdateTime = tBuffer.getLong();
        mSeqNo = tBuffer.getInt();
    }
}
