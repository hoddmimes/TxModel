package com.hoddmimes.txtest.aux.txlogger;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class TxlogReplayer {
    public static enum Direction {Forward, Backward}

    ;

    final private Direction mDirection;
    final private String mLogDir;
    final private String mServiceName;

    private List<String> mLogFilenames;
    private int mCurrentFileIndex;
    private FileReplayHandler mReplayer;
    private long mFromMessageSeqno;
    private TxlogReplyEntryInterface mNextReplayEntry;          // Next replay record

    public TxlogReplayer(String pLogDir, String pServiceName, Direction pDirection) {
        this(pLogDir, pServiceName, pDirection, 0L);
    }

    public TxlogReplayer(String pLogDir, String pServiceName, Direction pDirection, long pFromMessageSeqno) {
        this.mFromMessageSeqno = pFromMessageSeqno;
        this.mLogDir = pLogDir;
        this.mServiceName = pServiceName;
        this.mDirection = pDirection;
        List<TxLogfile> tTxLogfiles = TxlogAux.listTxlogFiles(mLogDir, mServiceName);

        if (tTxLogfiles.size() == 0) {
            mReplayer = null;
        } else {
            mLogFilenames = new ArrayList<>();
            if (pFromMessageSeqno > 0) {
                if (pDirection == Direction.Forward) {
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
                for (TxLogfile txlf : tTxLogfiles) {
                    mLogFilenames.add(txlf.getFullname());
                }
            }

            if (mLogFilenames.size() > 0) {
                this.mCurrentFileIndex = (mDirection == Direction.Forward) ? 0 : mLogFilenames.size() - 1;
                mReplayer = new FileReplayHandler(mLogFilenames.get(this.mCurrentFileIndex), this.mDirection, pFromMessageSeqno);
            } else {
                mReplayer = null;
            }
        }
    }

    public boolean hasMore() {
        return hasMore(WriteBuffer.FLAG_USER_PAYLOAD);
   }


    public boolean hasMore( int pRecordType ) {
        if (mNextReplayEntry == null) {
            mNextReplayEntry = next( pRecordType);
        }
        return (mNextReplayEntry != null);
    }

    public TxlogReplyEntryMessage next( ) {
        return (TxlogReplyEntryMessage) this.next( WriteBuffer.FLAG_USER_PAYLOAD)  ;
    }

    public TxlogReplyEntryInterface next( int pRecordType ) {

        if (mNextReplayEntry == null) {
            mNextReplayEntry = (mReplayer == null) ? null : this.nextMessage( pRecordType );
        }
        TxlogReplyEntryInterface re = mNextReplayEntry;
        mNextReplayEntry = null;
        return re;
    }


    TxlogReplyEntryInterface nextMessage( int pRecordType) {
        try {
            TxlogReplyEntryInterface tReplayEntry = mReplayer.nextMessage( pRecordType );
            if (tReplayEntry == null) {
                if (mDirection == Direction.Forward) {
                    mCurrentFileIndex++;
                    if (mCurrentFileIndex == mLogFilenames.size()) {
                        return null;
                    }
                    mReplayer.close();
                    mReplayer = new FileReplayHandler(mLogFilenames.get(this.mCurrentFileIndex), this.mDirection, this.mFromMessageSeqno);
                } else {
                    mCurrentFileIndex--;
                    if (mCurrentFileIndex < 0) {
                        return null;
                    }
                    mReplayer.close();
                    mReplayer = new FileReplayHandler(mLogFilenames.get(this.mCurrentFileIndex), this.mDirection, this.mFromMessageSeqno);
                }
                tReplayEntry = mReplayer.nextMessage( pRecordType );
            }
            return tReplayEntry;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            if (mReplayer != null) {
                mReplayer.close();
                mReplayer = null;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private class FileReplayHandler {
        private RandomAccessFile    mFile;
        private FileChannel         mFileChannel;
        private String              mFilename;
        private Direction           mDirection;
        private long                mFromMessageSeqno;
        private long                mCurrtPos;

        FileReplayHandler(String pFilename, Direction pDirection, long pMessageSeqno) {
            mFromMessageSeqno = pMessageSeqno;
            mFilename = (pFilename == null) ? "null" : pFilename;
            //System.out.println("Replaying file: " + pFilename + " direction: " + ((pDirection == TxlogReplayer.FORWARD) ? "FORWARD" : "BACKWARD") + " from seqno: " + mFromMessageSeqno );
            if (pFilename != null) {
                try {
                    mDirection = pDirection;
                    mFile = new RandomAccessFile(pFilename, "r");
                    mFileChannel = mFile.getChannel();
                    if (mDirection == Direction.Backward) {
                        mCurrtPos = TxlogAux.alignToEndOfTxlogFile(mFile); //after last record in file
                    } else {
                        mCurrtPos = 0;
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        void close() throws IOException {
            mFile.close();
        }

        TxlogReplyEntryInterface nextMessage( int pRecordType) {
            try {
                TxlogReplyEntryInterface tReplayEntry;
                do {
                    long tTotalRecordSize = getTotalRecordSize();
                    if (tTotalRecordSize == 0L) {
                        return null;
                    }
                    if (mDirection == Direction.Backward) {
                        mFileChannel.position( mCurrtPos - tTotalRecordSize);
                        tReplayEntry = nextMessageFromFile();
                        mCurrtPos -= tTotalRecordSize;
                    } else {
                         mFileChannel.position( mCurrtPos );
                         tReplayEntry = nextMessageFromFile();
                         mCurrtPos += tTotalRecordSize;
                    }
                } while( tReplayEntry.getType() != pRecordType);
                return  tReplayEntry;
            }
            catch( IOException e) {
                throw new RuntimeException(e);
            }
        }

        private long getTotalRecordSize() throws IOException {
            if (mDirection == Direction.Forward) {
                if ((mCurrtPos + WriteBuffer.TXLOG_PRE_HEADER_SIZE) > mFileChannel.size()) {
                    return 0L;
                }
                mFileChannel.position(mCurrtPos + Integer.BYTES);
                int tSize = (mFile.readInt() & WriteBuffer.PAYLOAD_LENGTH_MASK);
                if ((mCurrtPos + tSize) > mFileChannel.size()) {
                    return 0L;
                }
                return tSize + WriteBuffer.TXLOG_HEADER_SIZE;
            } else { // Backward
                if ((mCurrtPos - WriteBuffer.TXLOG_POST_HEADER_SIZE) < 0) {
                    return 0L;
                }
                mFileChannel.position(mCurrtPos -  WriteBuffer.TXLOG_POST_HEADER_SIZE);
                int tSize = (mFile.readInt() & WriteBuffer.PAYLOAD_LENGTH_MASK);
                if ((mCurrtPos - tSize) < 0) {
                    return 0L;
                }
                return tSize + WriteBuffer.TXLOG_HEADER_SIZE;
            }
        }

        private TxlogReplyEntryInterface nextMessageFromFile() throws IOException {
                if ((mFileChannel.position() + WriteBuffer.TXLOG_HEADER_SIZE) > mFileChannel.size()) {
                    return null; // EOF nothing more to read
                }
                int tMark = mFile.readInt();
                if (tMark != WriteBuffer.MAGIC_START_MARKER) {
                    throw new IOException("Invalid Start Marker found");
                }
                int tTypeAndLength1 = mFile.readInt();
                int tSize = (tTypeAndLength1 & WriteBuffer.PAYLOAD_LENGTH_MASK);
                int tType = (tTypeAndLength1 & WriteBuffer.REC_TYPE_MASK);

                //long  tFs  = mFileChannel.size();
                //long  tFp = mFileChannel.position();
                //long x = tFp + tSize + WriteBuffer.TXLOG_POST_HEADER_SIZE + Long.BYTES;
                if ((mFileChannel.position() + tSize + WriteBuffer.TXLOG_POST_HEADER_SIZE) > mFileChannel.size()) {
                    System.out.println("Warning: truncated record at end of file");
                    return null; // EOF nothing more to read
                }


                TxlogReplyEntryInterface tReplyEntry;

                if (tType == WriteBuffer.FLAG_PADDING_PAYLOAD) {
                    mFileChannel.position(mFileChannel.position() + tSize);
                    tReplyEntry = new TxlogReplyEntryFiller(tSize);
                } else if (tType == WriteBuffer.FLAG_USER_PAYLOAD) {
                    byte[] tPayload = new byte[tSize];
                    mFile.readFully(tPayload);
                    tReplyEntry = new TxlogReplyEntryMessage(tPayload, this.mFilename);
                } else {
                    byte[] tPayload = new byte[tSize];
                    mFile.readFully(tPayload);
                    tReplyEntry =  new TxlogReplyEntryStatistics(tPayload);
                }

                int tTypeAndLength2 = mFile.readInt();
                if (tTypeAndLength2 != tTypeAndLength1) {
                 throw new IOException("Invalid size/type in pre/post header");
    }
                tMark = mFile.readInt();
                if (tMark != WriteBuffer.MAGIC_END_MARKER) {
                    throw new IOException("Invalid End Marker found");
                }
                return tReplyEntry;

        }


    }
}


