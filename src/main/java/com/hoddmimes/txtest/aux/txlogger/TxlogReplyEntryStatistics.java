package com.hoddmimes.txtest.aux.txlogger;

public class TxlogReplyEntryStatistics implements TxlogReplyEntryInterface
{
    private long mLogTime;
    private int mMsgsInBuffer;
    private int mMsgsPayloadSize;
    private int mPreviousWriteTimeUsec;

    TxlogReplyEntryStatistics(byte[] pStatisticsData)  {
        mLogTime = bytesToLong(pStatisticsData, WriteBuffer.WRITE_STAT_OFFSET_LOG_TIME );
        mMsgsInBuffer = bytesToInt(pStatisticsData, WriteBuffer.WRITE_STAT_OFFSET_MSGS_IN_BUFFER);
        mMsgsPayloadSize = bytesToInt(pStatisticsData, WriteBuffer.WRITE_STAT_OFFSET_USER_PAYLOAD_SIZE);
        mPreviousWriteTimeUsec = bytesToInt(pStatisticsData, WriteBuffer.WRITE_STAT_OFFSET_WRTTIME_USEC);
    }

    @Override
    public int getType() {
        return WriteBuffer.FLAG_WRITE_STATISTICS_PAYLOAD;
    }

    public long getLogTime() {
        return mLogTime;
    }
    public int getMsgsInBuffer() {
        return mMsgsInBuffer;
    }
    public int getMsgsPayloadSize() {
        return mMsgsPayloadSize;
    }
    public int getPreviousWriteTimeUsec() {
        return mPreviousWriteTimeUsec;
    }


    private int bytesToInt(byte[] pBuffer, int pOffset )
    {
        int tValue = 0;
        tValue += ((pBuffer[ pOffset + 0] & 0xff) << 24);
        tValue += ((pBuffer[ pOffset + 1] & 0xff) << 16);
        tValue += ((pBuffer[ pOffset + 2] & 0xff) << 8);
        tValue += ((pBuffer[ pOffset + 3] & 0xff) << 0);
        return tValue;
    }

    private long bytesToLong(byte[] pBuffer, int pOffset )
    {
        long tValue = 0;
        tValue += (long) ((long)(pBuffer[ pOffset + 0] & 0xff) << 56);
        tValue += (long) ((long)(pBuffer[ pOffset + 1] & 0xff) << 48);
        tValue += (long) ((long)(pBuffer[ pOffset + 2] & 0xff) << 40);
        tValue += (long) ((long)(pBuffer[ pOffset + 3] & 0xff) << 32);
        tValue += (long) ((long)(pBuffer[ pOffset + 4] & 0xff) << 24);
        tValue += (long) ((long)(pBuffer[ pOffset + 5] & 0xff) << 16);
        tValue += (long) ((long)(pBuffer[ pOffset + 6] & 0xff) << 8);
        tValue += (long) ((long)(pBuffer[ pOffset + 7] & 0xff) << 0);
        return tValue;
    }


}
