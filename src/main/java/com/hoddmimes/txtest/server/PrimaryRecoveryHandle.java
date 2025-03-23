package com.hoddmimes.txtest.server;

import com.hoddmimes.txtest.aux.txlogger.TxLogger;
import com.hoddmimes.txtest.aux.txlogger.TxlogReplayRecord;
import com.hoddmimes.txtest.aux.txlogger.TxlogReplayer;
import com.hoddmimes.txtest.generated.ipc.messages.RecoveryData;
import com.hoddmimes.txtest.generated.ipc.messages.StandbyRecoveryRequest;
import com.hoddmimes.txtest.generated.ipc.messages.StandbyRecoveryResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PrimaryRecoveryHandle
{
    private static int MAX_RESPONSE_SIZE = (1024 * 64);
    private static int IdIndex = 1;
    private static final Logger mLogger = LogManager.getLogger(PrimaryRecoveryHandle.class);
    private TxlogReplayer txlogReplayer = null;
    private final int mId;
    private final TxLogger txLogger;


    PrimaryRecoveryHandle(TxLogger pTxLogger) {
        synchronized (this) {
            mId = IdIndex++;
            txLogger = pTxLogger;
        }
    }

    public int getId() {
        return mId;
    }


    // Invoked at the primare when the standby wants to recover an unprocessed primary transaction

    public StandbyRecoveryResponse recoverReplayAtPrimary(StandbyRecoveryRequest pFromStandbyRequest) {
        long tStartTime = System.currentTimeMillis();
        long tSeqno = pFromStandbyRequest.getLastKnownMessageSeqno() + 1;
        boolean eod = false;
        int tDataSize = 0;

        if (txlogReplayer == null) {
            txlogReplayer = txLogger.getReplayer(txLogger.getLogFilePattern(), TxlogReplayer.FORWARD, tSeqno);
        }

        StandbyRecoveryResponse tResponseToStandby = new StandbyRecoveryResponse();
        tResponseToStandby.setHandleId(mId);

        do {
            TxlogReplayRecord txl = txlogReplayer.next();
            // End of data, then all is done
            if (txl == null) {
                eod = true;
                break;
            }
            if (txl.getMsgSeqno() != tSeqno) {
                throw new RuntimeException("Recovery message seqno is out of phase, expected: " + tSeqno + " got: " + txl.getMsgSeqno());
            } else {
                tSeqno++;
            }

            byte[] tData = txl.getData();

            tDataSize += tData.length + 14;
            RecoveryData tRecoveryData = new RecoveryData();
            tRecoveryData.setData( tData );
            tRecoveryData.setMsgSeqno( txl.getMsgSeqno());
            tResponseToStandby.addMessageDataToArray(tRecoveryData);
            if (tDataSize >= MAX_RESPONSE_SIZE) {
                break;
            }
        } while( true );


        tResponseToStandby.setEndOfData( eod );
        int tSize = (tResponseToStandby.getMessageData() == null) ? 0 : tResponseToStandby.getMessageData().size();
        mLogger.info("[replayFromPrimary] start from seqno: " + pFromStandbyRequest.getLastKnownMessageSeqno()   +
                " to seqno: " + tSeqno  + " records in msg: " + tSize + " exec time: " + (System.currentTimeMillis() - tStartTime) + " ms.");
        return tResponseToStandby;
    }





}
