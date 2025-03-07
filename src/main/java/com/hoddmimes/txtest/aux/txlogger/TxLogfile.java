package com.hoddmimes.txtest.aux.txlogger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;

public class TxLogfile
{
   private String   mFullname;
   private long     mLastmodified;
   private long     mSize;
   private long     mFirstSequenceNumber;
   private long     mLastSequenceNumber;

   private RandomAccessFile mRandomAccessFile;
   private FileChannel mFileChannel;

   TxLogfile( String pLogfileFullname ) {
       mFullname = pLogfileFullname;
       getFileData();
   }

   private void getFileData() {
       File tFile = new File(mFullname );
       mLastmodified = tFile.lastModified();
       try {
           mRandomAccessFile = new RandomAccessFile(tFile, "r");
           mFileChannel = mRandomAccessFile.getChannel();
           mSize = mFileChannel.size();
           mFirstSequenceNumber = getFirstSeqno();
           mLastSequenceNumber = getLastSeqno();
           mFileChannel.close();
           mRandomAccessFile.close();
       } catch (IOException e) {
           throw new RuntimeException(e);
       }
   }

   private long getFirstSeqno() throws IOException {
       if (mSize < TxLogger.RECORD_HEADER_SIZE) {
           return 0L;
       }
       mFileChannel.position(Integer.BYTES);
       return mRandomAccessFile.readLong();
   }

    private long getLastSeqno() throws IOException {



        if (mSize < TxLogger.RECORD_HEADER_SIZE) {
            return 0L;
        }

       long tPos = alignToEndOfTxLog();
        while (true) {
            tPos -= (2 * Integer.BYTES);
            mFileChannel.position(tPos); // backup before END SIGN and Size/
            int tMsgSize = mRandomAccessFile.readInt();
            tPos -= tMsgSize; // Backup pointer to begining of message
            tPos -= Long.BYTES; // Backup pointer before messages sequence number
            mFileChannel.position(tPos);
            long tSeqNo = mRandomAccessFile.readLong();
            if (tSeqNo > 0) {
                return tSeqNo;
            }
            tPos -= Integer.BYTES; // Backup over message length, just after previous log record.
        }
    }

    private long alignToEndOfTxLog() throws IOException {
        long tPos = mFileChannel.size();
        tPos -= Integer.BYTES; // backup 4 bytes

        while(tPos > 0) {
            mFileChannel.position(tPos);
            if (mRandomAccessFile.readInt() == TxLogger.MAGIC_END) {
                return (tPos + Integer.BYTES);
            } else {
                tPos -= 1; // Backup one byte
            }
        }
        return 0L;
    }

    public String getFullname() {
        return mFullname;
    }

    public long getLastmodified() {
        return mLastmodified;
    }

    public long getSize() {
        return mSize;
    }

    public long getFirstSequenceNumber() {
        return mFirstSequenceNumber;
    }


    public long getLastSequenceNumber() {
        return mLastSequenceNumber;
    }

    public String toString() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
       return "file: " + mFullname + " size: " + mSize + " modified: " + simpleDateFormat.format(mLastmodified ) + " first: " + mFirstSequenceNumber + " last: " + mLastSequenceNumber;
    }

}
