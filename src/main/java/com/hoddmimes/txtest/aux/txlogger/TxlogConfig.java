package com.hoddmimes.txtest.aux.txlogger;

public class TxlogConfig
{
    private int mWriteBufferSize = 65536;      // 64 * 1024
    private int mWriteAlignSize = 512;
    private int mWriteQueueMaxLength = 250;
    private int mWriteQueueDrainSize = 64;
    private long mMaxLogfileSize = 5L * 1024L * 1024L;
    private String mWriteMode = "rws";
    private boolean mEnableWriteStatistics = false;



    public void setWriteBufferSize( int pSize ) {
        mWriteBufferSize = pSize;
    }


    public void setWriteBufferAlignSize( int pSize ) {
        mWriteAlignSize = pSize;
    }


    public void setLogfileMaxSize( long pSize ) {
        mMaxLogfileSize = pSize;
    }


    public void setWriteQueueMaxLength(int pSize) {
        mWriteQueueMaxLength = pSize;
    }


    public void setWiteQueueDrainSize(int pSize) {
        mWriteQueueDrainSize = pSize;
    }


    // https://docs.oracle.com/javase/7/docs/api/java/io/RandomAccessFile.html#RandomAccessFile(java.lang.String,%20java.lang.String)
    public void setWriteMode(String pWriteMode) {
        mWriteMode = pWriteMode;
    }


    public void setWriteStatistics(boolean pValue) {
        this.mEnableWriteStatistics = pValue;
    }


    public long getLogfileMaxSize() {
        return mMaxLogfileSize;
    }


    public int getWriteBufferSize() {
        return mWriteBufferSize;
    }


    public int getWriteBufferAlignSize() {
        return mWriteAlignSize;
    }


    public int getWriteQueueMaxLength() {
        return mWriteQueueMaxLength;
    }


    public int getWriteQueueDrainSize() {
        return mWriteQueueDrainSize;
    }


    public String getWriteMode() {
        return mWriteMode;
    }

    public boolean isWriteStatisticsEnabled() {
        return this.mEnableWriteStatistics;
    }
}
