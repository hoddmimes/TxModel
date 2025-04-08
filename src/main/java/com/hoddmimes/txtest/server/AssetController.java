package com.hoddmimes.txtest.server;

import com.google.gson.JsonObject;
import com.hoddmimes.txtest.aux.TxCntx;
import com.hoddmimes.txtest.aux.ipc.IpcController;
import com.hoddmimes.txtest.generated.fe.messages.RequestMessage;
import com.hoddmimes.txtest.generated.fe.messages.UpdateError;
import com.hoddmimes.txtest.generated.fe.messages.UpdateMessage;
import com.hoddmimes.txtest.generated.fe.messages.UpdateResponse;
import com.hoddmimes.txtest.generated.ipc.messages.FromStandby;
import com.hoddmimes.txtest.generated.ipc.messages.ToStandby;
import com.hoddmimes.txtest.quorum.QuorumStateController;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.LinkedBlockingQueue;


public class AssetController extends Thread implements ServerMessageSeqnoInterface
{
    private HashMap<Integer,Asset> mAssetMap;
    private Logger mLogger = LogManager.getLogger( AssetController.class);
    private TxServerIf mTxServerIf;

    private QuorumStateController qsController;
    private IpcController ipcController;
    private WaitingTxCntx mWaitingTxCntx;
    private boolean mWaitForStandbyResponse = true;
    private LinkedBlockingQueue<TxCntx> mStdbySendQueue;
    private int mExecChannels;
    public AssetController(TxServerIf pTxServerIf) {
        mTxServerIf = pTxServerIf;
        qsController = pTxServerIf.getQSController();
        ipcController = pTxServerIf.getIpcController();
        mWaitForStandbyResponse = pTxServerIf.getConfiguration().get( "wait_for_standby" ).getAsBoolean();
        mWaitingTxCntx = new WaitingTxCntx(mWaitForStandbyResponse);
        mExecChannels = pTxServerIf.getConfiguration().get( "executor_threads" ).getAsInt();
        mStdbySendQueue = new LinkedBlockingQueue<>();

        int tNumberOfAssets = pTxServerIf.getConfiguration().get("number_of_assets").getAsInt();

        JsonObject jTxLoggerConfig = pTxServerIf.getConfiguration().get("service").getAsJsonObject().get("tx_logging").getAsJsonObject();


        mAssetMap = new HashMap<>();
        for (int i = 0; i < tNumberOfAssets; i++) {
            Asset a = new Asset((i+1));
            mAssetMap.put( a.getAssetId(), a );
        }
        this.start();
    }

    private UpdateError createUpdateResponse(int pAssetId, int pRqstId, boolean pStatusOk, String pStatusText) {
        UpdateError rsp = new UpdateError();
        rsp.setStatusText(pStatusText);
        rsp.setRequestId( pRqstId );
        rsp.setStatusOk( pStatusOk );
        return rsp;
    }

    @Override
    public long getServerMessageSeqno() {
        return mTxServerIf.getMessageSequenceNumberIf().getServerMessageSeqno();
    }

    /**
     * Process a client transaction. In this simplified model we just have one type of transaction "UpdateMessage".
     * When entering the method is executing in a unique thread associated with the asset guarantying that no other execution involving the asset will execute in parallell.
     *
     * The transaction can be intialized in three different ways
     * 1) A frontend client has initiated the transaction. The transaction is executing in the context of a server being a primary.
     * 2) The transaction is replicated from the primary to a standby and is the executed in the context of a standby server. The aim is
     *    to update the business logic and save the tx to the standby tx logger with the aim to shadow the states and logs of the primary server.
     * 3) The server (at start) replayes the transactions from tx logfiles for recrating the latest knownstates of the business assets.
     *
     * The steps of the execution is as follows:
     * - Queue the transaction to the txlogger
     * - If primary queue the tx to the standby
     * - apply business logic
     * - if being the primary wait for a confirmation that the standby server have received the transaction
     *
     * @param pTxCntx
     */

    public void processClientMessage(TxCntx pTxCntx) {
        RequestMessage tRqstMsg = pTxCntx.getRequestMessage();
        int tAssetId = tRqstMsg.getAssetId();

        // Find the asset for wich the update is for
        Asset tAsset = mAssetMap.get(tAssetId);

        // Is there a known assset, if not send a reply to the client submitting the transaction
        if (tAsset == null) {
            pTxCntx.addTimestamp("start to send response (due to no asset found)");
            if (pTxCntx.isPrimaryTx()) { // Will only sent a response to the client if being in primary mode
                pTxCntx.sendResponse(createUpdateResponse(tAssetId, tRqstMsg.getRequestId(), false, "Unknown asset id (" + tAssetId + ")"));
                pTxCntx.addTimestamp("response to client sent");
            }
            mLogger.warn("Unknown asset id (" + tAssetId + ")");
            return;
        }

        // The only message affecting the business logic within this test is update messages
        if (tRqstMsg instanceof UpdateMessage) {
            UpdateMessage updMsg = (UpdateMessage) tRqstMsg;
            long tMessageSeqno = 0L;


            // Apply business logic
            pTxCntx.addTimestamp("Update business logic");
            int tCurrentAssetValue = tAsset.update(updMsg.getValue());
            pTxCntx.addTimestamp("Business logic updated");

            // if the transaction is executing in the context of a primary wait for the confirmation from the standby
            if ((!pTxCntx.isReplay()) && qsController.isFailoverMode() && (qsController.isPrimary() && (mWaitForStandbyResponse))) {
                pTxCntx.addTimestamp("Start waiting for standby reply");
                synchronized (pTxCntx) {
                    if (!pTxCntx.isReplicated()) {
                        try {
                            pTxCntx.wait(5000L);
                        } catch (InterruptedException e) {
                            mLogger.warn("No reply from STANDBY will continue");
                        }
                    }
                }

                pTxCntx.addTimestamp("Standby replication reply received");
            }

            // If being a tx from a frontend and executing as primary send back a response to the frontend client
            if (pTxCntx.isPrimaryTx()) {
                pTxCntx.addTimestamp("start to send response");
                // Send response to the client only if in primary mode
                UpdateResponse tUpdRsp = new UpdateResponse();
                tUpdRsp.setTxid( pTxCntx.getTxid());
                tUpdRsp.setRequestId(tRqstMsg.getRequestId());
                tUpdRsp.setStatusOk(true);
                tUpdRsp.setCurrentValue( tCurrentAssetValue);
                pTxCntx.sendResponse( tUpdRsp );
                pTxCntx.addTimestamp("response sent");
            } else {
                pTxCntx.addTimestamp("stdby business logic completed");
            }
            return;
        }
        pTxCntx.addTimestamp("start to send response");
        pTxCntx.sendResponse(createUpdateResponse(tAssetId, tRqstMsg.getRequestId(), true, "Unknown request message type: " + tRqstMsg.getClass().getName()));
        pTxCntx.addTimestamp("response sent");
        mLogger.warn("Unknown request message type: " + tRqstMsg.getClass().getName());
    }


    public void publishMessageToStandby( TxCntx pTxCntx ) {
        pTxCntx.addTimestamp("enter stdby logic");
        ToStandby toStandbyMsg = new ToStandby();
        toStandbyMsg.setSequenceNumber(pTxCntx.getMessageSequenceNumber());
        toStandbyMsg.setMessage(pTxCntx.getRequestMessage());
        toStandbyMsg.setTxid( pTxCntx.getTxid());
        mWaitingTxCntx.add(pTxCntx);
        if (!ipcController.send(qsController.getStandByNodeId(), toStandbyMsg)) {
            // Message was not sent to standby, due to not being present or failing
            // Set replication status to true so we avoid waiting for the standby confirmation
            pTxCntx.setReplicated( true );
        }
        pTxCntx.addTimestamp("Message to standby queued");
    }



    public void fromStandby(FromStandby pMessage) {
        if (mWaitForStandbyResponse) {
            TxCntx tTxCntx = mWaitingTxCntx.getAndRemove(pMessage.getSequenceNumber());
            if ((tTxCntx != null) && (tTxCntx.getMessageSequenceNumber() == pMessage.getSequenceNumber())) {
                tTxCntx.setReplicatedToStandby();
                if (mLogger.isTraceEnabled()) {
                    mLogger.trace("TX " + tTxCntx.getMessageSequenceNumber() + " is now replicated to STANDBY");
                }
            } else {
                mLogger.error("No outstanding transaction or no matching message sequence number");
            }
        } else {
            //if ((pMessage.getSequenceNumber() % 2000) == 0) {
            //    mLogger.info("primary seqno: " + getServerMessageSeqno() + " standby seqno: " + pMessage.getSequenceNumber() +
            //            "   diff: " + (getServerMessageSeqno() - pMessage.getSequenceNumber()));
            //}
        }
    }


    class WaitingTxCntx {
        volatile List<TxCntx> mWaitingTxCntxs;
        boolean mWaitForStandbyResponse;

        public WaitingTxCntx( boolean pWaitForStandbyResponse) {
            mWaitingTxCntxs = new ArrayList<>();
            mWaitForStandbyResponse = pWaitForStandbyResponse;
        }

        public synchronized void add(TxCntx pTxCntx) {
            if (mWaitForStandbyResponse) {
                mWaitingTxCntxs.add(pTxCntx);
            }
        }

        public synchronized void remove(TxCntx pTxCntx) {
            if (mWaitForStandbyResponse) {
                mWaitingTxCntxs.remove(pTxCntx);
            }
        }

        public synchronized TxCntx getAndRemove( long pMessageSequenceNumber )
        {
            if (!mWaitForStandbyResponse) {
                return null;
            }

            ListIterator<TxCntx> tItr = mWaitingTxCntxs.listIterator();
            while( tItr.hasNext() ) {
                TxCntx txCntx = tItr.next();
                if(txCntx.getMessageSequenceNumber() == pMessageSequenceNumber ) {
                    tItr.remove();
                    return txCntx;
                }
            }
            return null;
        }
    }
}
