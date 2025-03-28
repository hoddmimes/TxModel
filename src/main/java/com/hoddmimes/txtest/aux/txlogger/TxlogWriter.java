package com.hoddmimes.txtest.aux.txlogger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class TxlogWriter
{

    private String      mTxLogDir;
    private String      mTxLogServiceName;
    private TxlogConfig mConfiguration;


    private LinkedBlockingQueue<MsgQueItem> mQueue;
    private RandomAccessFile mFile;
    private FileChannel mFileChannel;
    private LogWriter mLogWriter;
    private ExecutorService mLogThread = Executors.newSingleThreadExecutor();
    private AtomicLong mTxlogFileMessageSeqno;
    private int mLastWriteTimeUsec = 0;


    public TxlogWriter(String pTxLogDir, String pTxLogServiceName, TxlogConfig pConfiguration) {
        mTxLogDir = pTxLogDir;
        mTxLogServiceName = pTxLogServiceName;
        mConfiguration = pConfiguration;
        mTxlogFileMessageSeqno = new AtomicLong( TxlogAux.getMessageSeqnoInTxlog(mTxLogDir, mTxLogServiceName));

        mQueue = new LinkedBlockingQueue<>();
        mFile = null;
        mLogWriter = new LogWriter();
        mLogThread.execute( mLogWriter );
    }

    public long getMessageSeqno() {
       return mTxlogFileMessageSeqno.get();
    }


    public synchronized long queueMessage( byte[] pMessage, long pMessageSeqno, long pTxid ) {
        return queueMessage(pMessage, pMessageSeqno, pTxid, null, null);
    }

    public synchronized long queueMessage( byte[] pMessage, long pMessageSeqno, long pTxid, TxlogWriteCallback pCallback, Object pParameter ) {
        MsgQueItem queitm = new MsgQueItem(pMessage, pMessageSeqno, pTxid, pCallback, pParameter);
        this.mQueue.add(queitm);
        return pMessageSeqno;
    }


    public synchronized long queueMessage( byte[] pMessage ) {
        return queueMessage(pMessage, null, null);
    }

    public synchronized long queueMessage( byte[] pMessage, TxlogWriteCallback pCallback, Object pParameter ) {
        long tSeqno = mTxlogFileMessageSeqno.incrementAndGet();
        MsgQueItem queitm = new MsgQueItem(pMessage, tSeqno, 0, pCallback, pParameter);
        this.mQueue.add(queitm);
        return tSeqno;
    }


    private void createNewRandomAccessFile() {
        if (mFile != null) {
            try {mFile.close();} catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        String tFileName = getFilename();
        try {
            mFile = new RandomAccessFile(tFileName,"rws");
        }
        catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String getFilename() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd_HHmmssSSS");
        String tDir = (mTxLogDir.endsWith(File.separator)) ? (mTxLogDir + "/") : mTxLogDir;
        return tDir + mTxLogServiceName + "-" + sdf.format(System.currentTimeMillis()) + ".txl";
    }

    class LogWriter implements Runnable
    {
        List<MsgQueItem> tMsgList = new ArrayList<>(mConfiguration.getWriteQueueDrainSize());
        WriteBuffer wrtBuffer = new WriteBuffer(mConfiguration.getWriteBufferSize(),mConfiguration.getWriteBufferAlignSize(), false);
        MsgQueItem msgitm;

        public LogWriter()
        {
        }

        @Override
        public void run() {

            //System.out.println("Starting LogWriter");
            try {
                while( true ) {
                    msgitm = mQueue.take();
                    msgToBuffer(msgitm);
                    if (!mQueue.isEmpty()) {
                        tMsgList.clear();
                        mQueue.drainTo(tMsgList);
                        for (MsgQueItem mqi : tMsgList) {
                            msgToBuffer(mqi);
                        }
                    }
                    writeBufferToFile(wrtBuffer);
                }
            }
            catch( Exception e) {
                e.printStackTrace();
            }
        }

        private void msgToBuffer( MsgQueItem pMsgItm ) {
            if (wrtBuffer.willDataFitInBuffer(pMsgItm.mMsg.length)) {
                // Message fits into the current buffer
                wrtBuffer.addData(pMsgItm.mMsg, pMsgItm.mMessageSeqno, pMsgItm.mTxid,  pMsgItm.mCallback,pMsgItm.mCallbackParameter);
                return;
            }

            // Current buffer will not fit the message, write current buffer to file
            // if the buffer holds any messages
            if (!wrtBuffer.isEmpty()) {
                writeBufferToFile( wrtBuffer );
            }

            // Will the message now fit into the (empty) write buffer  ?
            if (wrtBuffer.willDataFitInBuffer(pMsgItm.mMsg.length)) {
                wrtBuffer.addData(pMsgItm.mMsg, pMsgItm.mMessageSeqno, pMsgItm.mTxid, pMsgItm.mCallback,pMsgItm.mCallbackParameter);
                return;
            }

            // The message is larger than the default write buffer. A temporary larger buffer
            // is allocated to fit the message
            WriteBuffer tmpBuffer = new WriteBuffer(pMsgItm.totalMsgSize(), mConfiguration.getWriteBufferAlignSize(), true);
                tmpBuffer.addData(pMsgItm.mMsg, pMsgItm.mMessageSeqno, pMsgItm.mTxid,pMsgItm.mCallback,pMsgItm.mCallbackParameter);
                writeBufferToFile( tmpBuffer );
                tmpBuffer = null;
            }
        }

        private void openNewFile() {
            try {
                String tFilename = getFilename();
                if (mFileChannel != null) {
                    mFileChannel.force(true);
                    mFileChannel.close();
                }
                mFile = new RandomAccessFile( tFilename, "rws");
                mFileChannel = mFile.getChannel();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void writeBufferToFile( WriteBuffer pBuffer ) {
            if (pBuffer.isEmpty()) {
                return;
            }

            try {
                if ((mFileChannel == null) || (mFileChannel.position() >= mConfiguration.getLogfileMaxSize())) {
                    openNewFile();
                }

                pBuffer.setWriteStatistics( mLastWriteTimeUsec );
                long tStartTime = System.nanoTime();
                mFileChannel.write(pBuffer.getBuffer());
                mFileChannel.force(true);
                mLastWriteTimeUsec = (int) ((System.nanoTime() - tStartTime) / 1000L);


                pBuffer.executeCallbacks();
                //System.out.println("write buffer: " + pBuffer.getPosition());
                if (!pBuffer.isTempBuffer()) {
                    pBuffer.reset();
                } else {
                    pBuffer = null;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
    }

    static class MsgQueItem
    {
        MsgQueItem( byte[] pMessage,  long pMessageSeqno, long pTxid,  TxlogWriteCallback pCallback, Object pParameter) {
            mQueTime = (System.nanoTime() / 1000L);
            mMsg = pMessage;
            mCallback = pCallback;
            mCallbackParameter = pParameter;
            mMessageSeqno = pMessageSeqno;
            mTxid = pTxid;
        }

        int totalMsgSize() {
            return WriteBuffer.TXLOG_HEADER_SIZE + mMsg.length + Long.BYTES + Long.BYTES;
        }

        byte[]              mMsg;
        TxlogWriteCallback  mCallback;
        Object              mCallbackParameter;
        long                mQueTime;
        long                mMessageSeqno;
        long               mTxid;
    }

}
