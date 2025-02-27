package com.hoddmimes.txtest.aux.txlogger;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TxlogWriter
{
    private TxLogger mTxLogger;
    private LinkedBlockingQueue<MsgQueItem> mQueue;
    private RandomAccessFile mFile;
    private FileChannel mFileChannel;
    private int mLogfileSeqno = 0;
    private LogWriter mLogWriter;
    private ExecutorService mLogThread = Executors.newSingleThreadExecutor();




    public TxlogWriter(TxLogger pTxLogger ) {
        mTxLogger = pTxLogger;
        mQueue = new LinkedBlockingQueue<>();
        mFile = null;
        findLogfileSeqno();
        mLogWriter = new LogWriter();
        mLogThread.execute( mLogWriter );
    }


    public int queueMessage( byte[] pMessage ) {
        return queueMessage(pMessage, null, null);
    }

    public int queueMessage( byte[] pMessage, TxlogWriteCallback pCallback, Object pParameter ) {
        MsgQueItem queitm = new MsgQueItem(pMessage, pCallback, pParameter);
        this.mQueue.add(queitm);
        return this.mQueue.size();
    }



    private void findLogfileSeqno()
    {
        FileUtilParse fnp = new FileUtilParse(mTxLogger.mConfigLogFilePattern);
        List<String> tFilenames = fnp.listFilenames( true );

        if (!tFilenames.isEmpty()) {
            Pattern p = Pattern.compile(".+_(\\d+)\\." + fnp.getExtention());
            int tSeqNo = 1;
            for( String fn : tFilenames) {
                Matcher m = p.matcher( fn );
                if (m.matches()) {
                    int x = Integer.parseInt(m.group(1));
                    if (x > mLogfileSeqno) {
                        mLogfileSeqno = x;
                    }
                }
            }
        }
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
        SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd_HHmmss_SSS");
        if (mTxLogger.mConfigLogFilePattern.contains("#datetime#")) {
            return mTxLogger.mConfigLogFilePattern.replace("#datetime#", sdf.format(System.currentTimeMillis()));
        }
        FileUtilParse fnp = new FileUtilParse( mTxLogger.mConfigLogFilePattern);
        if (fnp.getExtention().length() == 0) {
            return  fnp.getFullname() + "_" + String.valueOf( ++mLogfileSeqno );
        } else {
            String tFilename = fnp.getFullname();
            return  tFilename.substring(0, tFilename.length() - (fnp.getExtention().length() + 1)) +
                    "_" + String.valueOf( ++mLogfileSeqno ) +  "." + fnp.getExtention();
        }
    }


    class LogWriter implements Runnable
    {
        List<MsgQueItem> tMsgList = new ArrayList<>(30);
        WriteBuffer wrtBuffer = new WriteBuffer(mTxLogger.mConfigBufferSize,mTxLogger.mConfigAlignSize, false);
        MsgQueItem msgitm;

        public LogWriter()
        {
        }

        @Override
        public void run() {

            System.out.println("Starting LogWriter");
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
            if (wrtBuffer.doesMsgFit(pMsgItm.mMsg)) {
                // Message fits into the current buffer
                wrtBuffer.put(pMsgItm.mMsg,pMsgItm.mCallback,pMsgItm.mCallbackParameter);
                return;
            }

            // Current buffer will not fit the message, write current buffer to file
            // if the buffer holds any messages
            if (wrtBuffer.getPosition() > 0) {
                writeBufferToFile( wrtBuffer );
            }

            // Will the message now fit into the (empty) write buffer  ?
            if (wrtBuffer.doesMsgFit(pMsgItm.mMsg)) {
                wrtBuffer.put(pMsgItm.mMsg,pMsgItm.mCallback,pMsgItm.mCallbackParameter);
                return;
            }

            // The message is larger than the default write buffer. A temporary larger buffer
            // is allocated to fit the message
            WriteBuffer tmpBuffer = new WriteBuffer(pMsgItm.totalMsgSize(), mTxLogger.mConfigAlignSize, true);
                tmpBuffer.put(pMsgItm.mMsg,pMsgItm.mCallback,pMsgItm.mCallbackParameter);
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
            if (pBuffer.getPosition() == 0) {
                return;
            }

            try {
                if ((mFileChannel == null) || (mFileChannel.position() >= mTxLogger.mConfigMaxFileSize)) {
                    openNewFile();
                }
                System.out.println("pre-write buffer: " + pBuffer.getPosition());
                pBuffer.align();
                System.out.println("align buffer: " + pBuffer.getPosition());
                pBuffer.flip();
                mFileChannel.write( pBuffer.getBuffer());
                mFileChannel.force(true);
                pBuffer.executeCallbacks();
                System.out.println("write buffer: " + pBuffer.getPosition());
                pBuffer.clear();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
    }

    static class MsgQueItem
    {
        MsgQueItem( byte[] pMessage, TxlogWriteCallback pCallback, Object pParameter ) {
            mQueTime = (System.nanoTime() / 1000L);
            mMsg = pMessage;
            mCallback = pCallback;
            mCallbackParameter = pParameter;
        }

        int totalMsgSize() {
            return TxLogger.RECORD_HEADER_SIZE + mMsg.length;
        }

        byte[]              mMsg;
        TxlogWriteCallback  mCallback;
        Object              mCallbackParameter;
        long                mQueTime;
    }

}
