package com.hoddmimes.txtest.server;

import com.hoddmimes.txtest.aux.txlogger.TxLogger;
import com.hoddmimes.txtest.aux.txlogger.TxlogReplayer;
import com.hoddmimes.txtest.aux.txlogger.TxlogReplyEntryMessage;
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
    private final String mLogDir;
    private final String mServiceName;




    PrimaryRecoveryHandle(TxServerIf pTxServerIf) {
        synchronized (this) {
            mId = IdIndex++;
            mServiceName = pTxServerIf.getServiceName();
            mLogDir = "./" + String.format("%02d",pTxServerIf.getNodeId()) + "/";

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
            txlogReplayer = TxLogger.getReplayer( mLogDir, mServiceName, TxlogReplayer.Direction.Forward, tSeqno);
        }

        StandbyRecoveryResponse tResponseToStandby = new StandbyRecoveryResponse();
        tResponseToStandby.setHandleId(mId);

        do {
            TxlogReplyEntryMessage txl = txlogReplayer.next();
            // End of data, then all is done
            if (txl == null) {
                eod = true;
                break;
            }
            if (txl.getMessageSeqno() != tSeqno) {
                throw new RuntimeException("Recovery message seqno is out of phase, expected: " + tSeqno + " got: " + txl.getMessageSeqno());
            } else {
                tSeqno++;
            }

            byte[] tData = txl.getMsgPayload();

            tDataSize += tData.length + 14;
            RecoveryData tRecoveryData = new RecoveryData();
            tRecoveryData.setData( tData );
            tRecoveryData.setMsgSeqno( txl.getMessageSeqno());
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
