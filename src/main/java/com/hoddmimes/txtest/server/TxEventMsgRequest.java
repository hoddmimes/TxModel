package com.hoddmimes.txtest.server;

import com.hoddmimes.txtest.aux.AuxTimestamp;
import com.hoddmimes.txtest.aux.TxCntx;
import org.apache.logging.log4j.Logger;


public class TxEventMsgRequest extends TxExecutor.TxRunnable {
    private TxCntx      mTxCntx;
    private TxServer    mTxServer;
    private Logger      mLogger;
    private long        mTxStartTime;
    public TxEventMsgRequest(TxServer pTxServer, TxCntx pTxCntx, Logger pLogger) {
        mTxCntx = pTxCntx;
        mTxServer = pTxServer;
        mLogger = pLogger;
    }
    @Override
    public void execute()
    {
        mTxStartTime = System.nanoTime();
        mTxCntx.addTimestamp("start TX execution");
        mTxServer.processClientMessage( mTxCntx );
        mTxCntx.addTimestamp("end TX execution");

        if (mLogger.isTraceEnabled()) {
            mLogger.trace(mTxCntx.mTimestamps.toString() + "\n\n");
        } else {
            if ((!AuxTimestamp.isDisabled()) && (mTxCntx.mTimestamps.getTotalTimeUsec() >= 1500L)) {
                mLogger.warn(mTxCntx.mTimestamps.toString() + "\n\n");
            }
        }
        mTxServer.transactionCompleted(mTxCntx, mTxStartTime);
    }
}
