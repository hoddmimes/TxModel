package com.hoddmimes.txtest.server;

import com.hoddmimes.txtest.aux.TxCntx;

public class TxEventMsgRequest extends TxExecutor.TxRunnable {
    private TxCntx      mTxCntx;
    private TxServer    mTxServer;

    public TxEventMsgRequest( TxServer pTxServer,  TxCntx pTxCntx ) {
        mTxCntx = pTxCntx;
        mTxServer = pTxServer;
    }
    @Override
    public void execute()
    {
        mTxCntx.addTimestamp("start TX execution");
        mTxServer.processClientMessage( mTxCntx );
        mTxCntx.addTimestamp("end TX execution");
        System.out.println( mTxCntx.mTimestamps.toString() + "\n\n");
    }
}
