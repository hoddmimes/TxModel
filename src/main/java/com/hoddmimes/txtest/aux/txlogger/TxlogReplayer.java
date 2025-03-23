package com.hoddmimes.txtest.aux.txlogger;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class TxlogReplayer {
    public static int FORWARD = 0;
    public static int BACKWARD = 1;

    private int mDirection;
    private String mFilenamePattern;
    private List<String> mLogFilenames;
    private int mCurrentFileIndex;
    private FileReplayHandler mReplayHandler;
    private long mFromMessageSeqno;


    public TxlogReplayer(String pFilenamePattern, int pDirection) {
        this(pFilenamePattern, pDirection, -1L);
    }

    public TxlogReplayer(String pFilenamePattern, int pDirection, long pFromMessageSeqno) {
        this.mFromMessageSeqno = pFromMessageSeqno;
        this.mFilenamePattern = pFilenamePattern;
        this.mDirection = pDirection;
        List<TxLogfile> tTxLogfiles = TxLogger.listPatternTxLogfiles(pFilenamePattern);

        if (tTxLogfiles.size() == 0) {
            mReplayHandler = null;
        } else {
            mLogFilenames = new ArrayList<>();
            if (pFromMessageSeqno >= 0) {
                if (pDirection == FORWARD) {
                    for (TxLogfile tTxLogfile : tTxLogfiles) {
                        if (pFromMessageSeqno <= tTxLogfile.getLastSequenceNumber()) {
                            mLogFilenames.add(tTxLogfile.getFullname());
                        }
                    }
                } else { // backward
                    for (TxLogfile tTxLogfile : tTxLogfiles) {
                        if (pFromMessageSeqno >= tTxLogfile.getFirstSequenceNumber()) {
                            mLogFilenames.add(tTxLogfile.getFullname());
                        }
                    }
                }
            } else {
                listLogfiles();
            }

            if (mLogFilenames.size() > 0) {
                this.mCurrentFileIndex = (mDirection == FORWARD) ? 0 : mLogFilenames.size() - 1;
                mReplayHandler = new FileReplayHandler(mLogFilenames.get(this.mCurrentFileIndex), this.mDirection, pFromMessageSeqno );
            } else {
                mReplayHandler = null;
            }
            }
    }



    public  TxlogReplayRecord next() {
        if (mReplayHandler == null) {
            return null;
        }

        try {

            TxlogReplayRecord tRec = mReplayHandler.nextRecord();
            if (tRec == null) {
                if (mDirection == TxlogReplayer.FORWARD) {
                    mCurrentFileIndex++;
                    if (mCurrentFileIndex == mLogFilenames.size()) {
                        return null;
                    }
                    mReplayHandler.close();
                    mReplayHandler = new FileReplayHandler(mLogFilenames.get(this.mCurrentFileIndex), this.mDirection, this.mFromMessageSeqno);
                }
                else {
                    mCurrentFileIndex--;
                    if (mCurrentFileIndex < 0) {
                        return null;
                    }
                    mReplayHandler.close();
                    mReplayHandler = new FileReplayHandler(mLogFilenames.get(this.mCurrentFileIndex), this.mDirection, this.mFromMessageSeqno);
                }
                tRec = mReplayHandler.nextRecord();
            }
            return tRec;
        }
        catch( IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            if (mReplayHandler != null) {
                mReplayHandler.close();
                mReplayHandler = null;
            }
        }
        catch( IOException e) {
            e.printStackTrace();
        }
    }



    private void listLogfiles() {
        FileUtilParse fnp = new FileUtilParse( this.mFilenamePattern);
        mLogFilenames = fnp.listWildcardFiles();

    }

    private class FileReplayHandler
    {
        private RandomAccessFile mFile;
        private FileChannel mFileChannel;
        private String  mFilename;
        private int mDirection;
        private long mPos;
        private long mFromMessageSeqno;


        FileReplayHandler( String pFilename, int pDirection, long pMessageSeqno ) {
            mFromMessageSeqno = pMessageSeqno;
            mFilename = (pFilename == null) ? "null" : pFilename;
            //System.out.println("Replaying file: " + pFilename + " direction: " + ((pDirection == TxlogReplayer.FORWARD) ? "FORWARD" : "BACKWARD") + " from seqno: " + mFromMessageSeqno );
            if (pFilename != null) {
                try {
                    mDirection = pDirection;
                    mFile = new RandomAccessFile(pFilename, "r");
                    mFileChannel = mFile.getChannel();
                    if (mDirection == TxlogReplayer.BACKWARD) {
                        alignAtLastRecord();
                    } else {
                        mPos = 0;
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {

            }
        }

        void close() throws IOException{
            mFile.close();
        }

        private int getTotalRecordSize() throws IOException
        {
             int tTotSize = 0;
             long tPos = mFileChannel.position();

             if (mDirection == TxlogReplayer.FORWARD) {
                 if (mFileChannel.position() == mFileChannel.size()) {
                     tTotSize = 0;
                 } else {
                     tTotSize = TxLogger.RECORD_HEADER_SIZE + mFile.readInt();
                 }
             } else {
                 if (mFileChannel.position() <= 2 * Integer.BYTES) {
                     tTotSize = 0;
                 } else {
                     mFileChannel.position(tPos - 2 * Integer.BYTES);
                     tTotSize = TxLogger.RECORD_HEADER_SIZE + mFile.readInt();
                 }
             }
             mFileChannel.position( tPos );
             return tTotSize;
        }


        private TxlogReplayRecord readTxlRecord() throws IOException {

            // Read message size
            int tMsgSize1 = mFile.readInt();

            // Read message logging sequence number. If 0 (zero) it is a filler record that should not be replayed
            long tMsgSeqno = mFile.readLong();
            if (tMsgSeqno < 0) {
                throw new IOException("Invalid reply option found in file\"" + mFilename + " at position " + mPos);
            }

            byte[] tMsgData = new byte[tMsgSize1];
            mFile.read( tMsgData );

            int tMsgSize2 = mFile.readInt();
            if (tMsgSize1 != tMsgSize2) {
                throw new IOException("Invalid size, size1 <> size2 file\"" + mFilename + " size1 = " + tMsgSize1 + " size2 = " + tMsgSize2);
            }

            int tMagicEnd = mFile.readInt();
            if (tMagicEnd != TxLogger.MAGIC_END) {
                throw new IOException("END Markee not found in file\"" + mFilename + " at position " + mPos);
            }

            return new TxlogReplayRecord( tMsgData, mFilename, tMsgSeqno);
        }


        private TxlogReplayRecord nextRecord() throws IOException {

            TxlogReplayRecord txlrec = null;

            while( true ) {
                int tTotRecSize = getTotalRecordSize();

                if (mDirection == TxlogReplayer.BACKWARD) {
                    if (mPos <= 0) { // At the beginning of the file, no more data available in this file
                        return null;
                    }

                    // Backup the file pointer to the begining of the record to be read
                    mPos -= tTotRecSize;
                    mFileChannel.position(mPos);

                    // Read the record, which will move the filepointer forward
                    txlrec = readTxlRecord();
                    // Backup the pointer to ahead of the read record
                    mFileChannel.position(mPos);
                } else {
                    if (mPos + tTotRecSize >= mFileChannel.size()) { // Passing the end of the file
                        return null;
                    }
                    txlrec = readTxlRecord();
                    mPos += tTotRecSize;
                }

                if (!txlrec.isIgnored() &&
                        (  (mFromMessageSeqno == -1) ||
                           ((mDirection == FORWARD) && (mFromMessageSeqno <= txlrec.getMsgSeqno())) ||
                           ((mDirection == BACKWARD) && (txlrec.getMsgSeqno() <= mFromMessageSeqno))
                        )
                    )
                {
                    return txlrec;
                }
            }
        }


        private void alignAtLastRecord() throws IOException {
            mPos = mFileChannel.size();
            mPos -= 4; // backup 4 bytes
            mFileChannel.position(mPos);

            while(mPos > 0) {
                mFileChannel.position(mPos);
                if (mFile.readInt() == TxLogger.MAGIC_END) {
                    mPos += 4;
                    return;
                } else {
                    mPos -= 1; // Backup one byte
                }
            }
            System.out.println("error: could not found last record in the replay file \"" + mFilename + "\"");
        }
    }
}
