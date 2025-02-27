package com.hoddmimes.txtest.server;

import com.hoddmimes.txtest.aux.TxCntx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

public class TxExecutor
{
    private LinkedBlockingDeque<TxRunnable> mQueues[];
    private ExecutorService mThreadPool;
    private Logger mLogger;
    private TxServer mTxServer;


    public TxExecutor( TxServer pTxServer, int pThreadCount ) {
        mLogger = LogManager.getLogger( TxExecutor.class );
        mTxServer = pTxServer;
        mQueues = new LinkedBlockingDeque[pThreadCount];
        ExecutorService mThreadPool = Executors.newFixedThreadPool( pThreadCount);
        for(int i = 0; i < pThreadCount; i++) {
            mQueues[i] = new LinkedBlockingDeque<>();
            mThreadPool.execute( new TxThread( i ));
        }
    }

    public void queueRequest(TxCntx pTxCntx) {
        int tQueueIndex = pTxCntx.mRequest.getAssetId() % mQueues.length;
        mQueues[tQueueIndex].add( new TxEventMsgRequest( mTxServer, pTxCntx ));

    }

    class TxThread implements Runnable {
        private int mThreadIndex;

        public TxThread( int pThreadIndex ) {
            mThreadIndex = pThreadIndex;
        }

        @Override
        public void run() {
            mLogger.info("Starting executor thread " + mThreadIndex );
            while( true ) {
                try {
                    TxRunnable tTxRunnable = mQueues[mThreadIndex].take();
                    tTxRunnable.execute();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static abstract class TxRunnable
    {
        public abstract void execute();
    }


}
