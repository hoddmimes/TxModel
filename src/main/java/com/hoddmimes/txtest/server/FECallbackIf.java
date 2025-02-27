package com.hoddmimes.txtest.server;

import com.hoddmimes.txtest.aux.TxCntx;

public interface FECallbackIf
{
    public void queueInboundClientMessage(TxCntx pTxCntx);
}
