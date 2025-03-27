package com.hoddmimes.txtest.aux.txlogger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;

public class TxLogfile implements Comparable<TxLogfile> {
    private String mFullname;
    private long mLastmodified;
    private long mSize;
    private long mFirstSequenceNumber;
    private long mLastSequenceNumber;

    private RandomAccessFile mRandomAccessFile;
    private FileChannel mFileChannel;

    TxLogfile(String pLogfileFullname) {
        mFullname = pLogfileFullname;
        getFileData();
    }

    private void getFileData() {
        File tFile = new File(mFullname);
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
        if (mSize < WriteBuffer.TXLOG_HEADER_SIZE) {
            return 0L;
        }

        while( true ) {
            if (mFileChannel.position() >= mFileChannel.size()) {
                return -1L;
            }
            mRandomAccessFile.readInt(); // Read begin marker
            int tTypeAndSize = mRandomAccessFile.readInt();
            int tSize = (tTypeAndSize & WriteBuffer.PAYLOAD_LENGTH_MASK);
            int tType = (tTypeAndSize & WriteBuffer.REC_TYPE_MASK);
            if (tType == WriteBuffer.FLAG_USER_PAYLOAD) {
                return mRandomAccessFile.readLong();
            } else {
                mFileChannel.position(mFileChannel.position() + tSize + WriteBuffer.TXLOG_POST_HEADER_SIZE);
            }
        }
    }

    private long getLastSeqno() throws IOException {

        if (mSize < WriteBuffer.TXLOG_HEADER_SIZE) {
            return 0L;
        }

        long tPos = TxlogAux.alignToEndOfTxlogFile(mRandomAccessFile); // Positioned after last valid record in the txl
        while (true) {
            tPos -= (WriteBuffer.TXLOG_POST_HEADER_SIZE); // backup to the beginning of the post header
            mFileChannel.position(tPos); // backup before END SIGN and Size/
            int tUserDataMsgSize = (mRandomAccessFile.readInt() & WriteBuffer.PAYLOAD_LENGTH_MASK);
            tPos -= tUserDataMsgSize + WriteBuffer.TXLOG_PRE_HEADER_SIZE; // Backup pointer to beginning of the record
            mFileChannel.position(tPos); // Position the pointer to beginning of the record
            int tMarker = mRandomAccessFile.readInt(); // Read begin marker
            int tRecType = mRandomAccessFile.readInt() & WriteBuffer.REC_TYPE_MASK; // Read the record type
            if (tRecType == WriteBuffer.FLAG_USER_PAYLOAD) {
                return mRandomAccessFile.readLong(); // read message seqno
            }
        }
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
        return "file: " + mFullname + " size: " + mSize + " modified: " + simpleDateFormat.format(mLastmodified) + " first: " + mFirstSequenceNumber + " last: " + mLastSequenceNumber;
    }


    @Override
    public int compareTo(TxLogfile f) {
        if (this.getFirstSequenceNumber() != f.getFirstSequenceNumber()) {
            return Long.compare(this.getFirstSequenceNumber(), f.getFirstSequenceNumber());
        }
        if (this.getLastSequenceNumber() != f.getLastSequenceNumber()) {
            return Long.compare(this.getLastSequenceNumber(), f.getLastSequenceNumber());
        }
        return 0;
    }
}
