package com.hoddmimes.txtest.aux.txlogger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


     /*
        Records present and sequensed in the
        write buffer. Can be uf three types
        - User data payload
        - Padding data , in the end of the writte buffer
        - Write statistics, in the beging of the write buffer

      +-------------------------------------+
      | Start Marker 4 bytes, 0xE1E1E1E1    |
      +------------------------------------+
      | flags 4 bits, user data  len 28 bit |
      +-------------------------------------+
      | logfile msg seqno 64 bits           |
      +-------------------------------------+
      | Data payload 'n' bytes              |
      +-------------------------------------+
      | flags 4 bits, user data  len 28 bit |         |
      +-------------------------------------+
      | End Marker 4 bytes, 0xE9E9E9E9      |
      +-------------------------------------+
     */


    /*
        Write statistics record. Always present and
        first 'message' of write buffer data
      +-----------------------------------------+
      | Start Marker 4 bytes, 0xE1E1E1E1        |
      ------------------------------------------+
      | flags 4 bits, user data  len 28 bit     | FLAG_WRITE_STATISTICS_PAYLOAD + TXLOG_WRITE_STATISTIC_LENGTH
      +-----------------------------------------+
      | Time when written to file, 8 bytes      |
      +-----------------------------------------+
      | User pay load msgs in buffer 4 bytes    |
      +-----------------------------------------+
      | Size of the user payload  data 4 byte   |
      +-----------------------------------------+
      | Previous write time in usec 4 bytes     |
      +-----------------------------------------+
      | flags 4 bits, user data  len 28 bit     | FLAG_WRITE_STATISTICS_PAYLOAD + TXLOG_WRITE_STATISTIC_LENGTH
      +-----------------------------------------+
      | End Marker 4 bytes, 0xE9E9E9E9          |
      +-----------------------------------------+
    */





public class WriteBuffer
{
    protected static final int MAGIC_START_MARKER = 0xE1E1E1E1;
    protected static final int MAGIC_END_MARKER   = 0xE9E9E9E9;
    protected static final byte PAD_BYTE = (byte) 'z';

    protected static final int TXLOG_POST_HEADER_SIZE = 8;
    protected static final int TXLOG_PRE_HEADER_SIZE  = 8;


    protected static final int TXLOG_HEADER_SIZE = TXLOG_PRE_HEADER_SIZE + TXLOG_POST_HEADER_SIZE;
    protected static final int MAX_ALIGN_SIZE = 8192;


    public static final int FLAG_USER_PAYLOAD               = 0x80000000;
    protected static final int FLAG_PADDING_PAYLOAD            = 0x40000000;
    public static final int FLAG_WRITE_STATISTICS_PAYLOAD   = 0x20000000;
    protected static final int PAYLOAD_LENGTH_MASK             = 0x0fffffff;
    protected static final int REC_TYPE_MASK                   = 0xf0000000;

    // Stat record offsets
    protected static int WRITE_STAT_OFFSET_LOG_TIME = 0;   // Time when the buffer was written to logfile
    protected static int WRITE_STAT_OFFSET_MSGS_IN_BUFFER = WRITE_STAT_OFFSET_LOG_TIME + Long.BYTES; // Number of user data messsages in buffer
    protected static int WRITE_STAT_OFFSET_USER_PAYLOAD_SIZE= WRITE_STAT_OFFSET_MSGS_IN_BUFFER + Integer.BYTES;
    protected static int WRITE_STAT_OFFSET_WRTTIME_USEC = WRITE_STAT_OFFSET_USER_PAYLOAD_SIZE + Integer.BYTES;
    protected static int TXLOG_STATISTICS_SIZE = WRITE_STAT_OFFSET_WRTTIME_USEC + Integer.BYTES;


    private ByteBuffer               mBuffer;
    private List<Callback>           mCallbacks;
    private boolean                  mIsTempBuffer;
    private int                      mMsgsInBuffer;
    private int                      mUserDataSize;
    private int                      mAlignSize;


    private static byte[] cFiller = new byte[10240];
    static {
        Arrays.fill(cFiller, (byte) 'z');
    }



    public WriteBuffer(int pBufferSize, int pAlignSize, boolean pIsTempBuffer)
    {
        mAlignSize = pAlignSize;
        mBuffer = ByteBuffer.allocateDirect( pBufferSize +  pAlignSize);
        mIsTempBuffer = pIsTempBuffer;
        mCallbacks = null;
        mMsgsInBuffer = 0;
        mUserDataSize = 0;
        reset();
    }

    public void reset() {
        this.mMsgsInBuffer = 0;
        this.mUserDataSize = 0;
        this.mCallbacks = null;
        mBuffer.clear();
        mBuffer.position( TXLOG_HEADER_SIZE + TXLOG_STATISTICS_SIZE );
    }

    public boolean isEmpty() {
        return (mBuffer.position() <= TXLOG_HEADER_SIZE + TXLOG_STATISTICS_SIZE);
    }
    public boolean isTempBuffer() {
        return mIsTempBuffer;
    }

    public void addData(byte[] pMessage, long pSequenceNo ) {
        this.addData( pMessage,  pSequenceNo, 0L,  null, null);
    }

    public void addData(byte[] pMessage, long pSequenceNo, long pTxid, TxlogWriteCallback pCallback, Object pCallbackParameter ) {
        this.mMsgsInBuffer++;
        this.mUserDataSize += pMessage.length;

        mBuffer.putInt( MAGIC_START_MARKER);
        mBuffer.putInt( FLAG_USER_PAYLOAD + (pMessage.length + Long.BYTES + Long.BYTES ));
        mBuffer.putLong( pSequenceNo );
        mBuffer.putLong( pTxid );
        mBuffer.put( pMessage );
        mBuffer.putInt( FLAG_USER_PAYLOAD + (pMessage.length + Long.BYTES + Long.BYTES));
        mBuffer.putInt( MAGIC_END_MARKER);
        if (pCallback != null) {
            if (this.mCallbacks == null) {
                this.mCallbacks = new ArrayList<>();
            }
            this.mCallbacks.add( new Callback( pCallback, pCallbackParameter));
        }
    }

    public static WriteBuffer allocateLargeWriteBuffer( int pUserDatalength ) {
        int tTotSize = pUserDatalength + (3 * TXLOG_HEADER_SIZE) + TXLOG_STATISTICS_SIZE;
        return new WriteBuffer( tTotSize, 512, true);
    }

    public void setWriteStatistics( int pPreviousWriteTimeUsec ) {
        // Header mark + flags + length
        mBuffer.putInt(0, MAGIC_START_MARKER);
        mBuffer.putInt( Integer.BYTES, TXLOG_STATISTICS_SIZE + FLAG_WRITE_STATISTICS_PAYLOAD);


        mBuffer.putLong( TXLOG_PRE_HEADER_SIZE + WRITE_STAT_OFFSET_LOG_TIME, System.currentTimeMillis());
        mBuffer.putInt( TXLOG_PRE_HEADER_SIZE + WRITE_STAT_OFFSET_MSGS_IN_BUFFER, this.mMsgsInBuffer );
        mBuffer.putInt( TXLOG_PRE_HEADER_SIZE + WRITE_STAT_OFFSET_USER_PAYLOAD_SIZE, this.mUserDataSize );
        mBuffer.putInt( TXLOG_PRE_HEADER_SIZE + WRITE_STAT_OFFSET_WRTTIME_USEC, pPreviousWriteTimeUsec);

        mBuffer.putInt( TXLOG_PRE_HEADER_SIZE + TXLOG_STATISTICS_SIZE, TXLOG_STATISTICS_SIZE + FLAG_WRITE_STATISTICS_PAYLOAD);
        mBuffer.putInt(  TXLOG_PRE_HEADER_SIZE + TXLOG_STATISTICS_SIZE + Integer.BYTES, MAGIC_END_MARKER);
    }

    public boolean willDataFitInBuffer(int pMsgSize )
    {
        return  ((mBuffer.capacity() - (2 * TXLOG_HEADER_SIZE) - mBuffer.position() - (2 * pMsgSize - Long.BYTES )) > 0);
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


    public ByteBuffer getBuffer(){
        int tPadSize = mAlignSize - ((mBuffer.position() + TXLOG_HEADER_SIZE) - (mAlignSize * ((mBuffer.position() + TXLOG_HEADER_SIZE) / mAlignSize)));
        mBuffer.putInt(MAGIC_START_MARKER);
        mBuffer.putInt(FLAG_PADDING_PAYLOAD + tPadSize);
        mBuffer.put(cFiller, 0, tPadSize);
        mBuffer.putInt(FLAG_PADDING_PAYLOAD + tPadSize);
        mBuffer.putInt(MAGIC_END_MARKER);

        mBuffer.flip();
        return mBuffer;
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
