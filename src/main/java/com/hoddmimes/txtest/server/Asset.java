package com.hoddmimes.txtest.server;

import java.nio.ByteBuffer;

public class Asset
{
    private int     mAssetId;
    private int     mValue;
    private long    mUpdateCount;


    public Asset( int pAssetId ) {
        mAssetId = pAssetId;
        mValue = 0;
        mUpdateCount = 0;

    }

    public int getAssetId() {
        return mAssetId;
    }

    public int getValue() {
        return mValue;
    }

    public long getUpdateCount() {
        return mUpdateCount;
    }

    public void update( int pUpdateValue ) {
        mValue += pUpdateValue;
        mUpdateCount++;
    }

    public byte[] encode() {
        ByteBuffer tBuffer = ByteBuffer.allocate(16);
        tBuffer.putInt( mAssetId );
        tBuffer.putInt( mValue );
        tBuffer.putLong( mUpdateCount );
        return tBuffer.array();
    }

    public void decode( byte[] pBuffer ) {
        ByteBuffer tBuffer = ByteBuffer.wrap( pBuffer );
        mAssetId = tBuffer.getInt();
        mValue = tBuffer.getInt();
        mUpdateCount = tBuffer.getLong();
    }
}
