package com.hoddmimes.txtest.server;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hoddmimes.msgcompiler.messaging.MessageInterface;
import com.hoddmimes.txtest.aux.AuxTimestamp;
import com.hoddmimes.txtest.aux.TxCntx;
import com.hoddmimes.txtest.aux.fe.FEController;
import com.hoddmimes.txtest.aux.ipc.IpcCallbacks;
import com.hoddmimes.txtest.aux.ipc.IpcCntx;
import com.hoddmimes.txtest.aux.ipc.IpcController;
import com.hoddmimes.txtest.aux.ipc.IpcNode;
import com.hoddmimes.txtest.aux.txlogger.TxLogger;
import com.hoddmimes.txtest.aux.txlogger.TxlogReplayRecord;
import com.hoddmimes.txtest.aux.txlogger.TxlogReplayer;
import com.hoddmimes.txtest.generated.fe.messages.FEFactory;
import com.hoddmimes.txtest.generated.fe.messages.RequestMessage;
import com.hoddmimes.txtest.generated.fe.messages.UpdateMessage;
import com.hoddmimes.txtest.generated.ipc.messages.*;
import com.hoddmimes.txtest.quorum.QuorumStateController;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class TxServer implements IpcCallbacks, FECallbackIf, ServerMessageSeqnoInterface
{
    private enum ServerState {Recovery, Synchronizing, Synchronized};

    private Logger mLogger = LogManager.getLogger(TxServer.class);
    private JsonObject mConfiguration;
    private String mConfigFilename = "./TxServer.json"; // Default configuration
    private int mNodeId = 0;
    TxExecutor mTxExecutor;
    FEController mFEController;
    QuorumStateController mQSController;
    AssetController mAssetController;
    IpcController mIpcController;
    TxLogger mTxLogger;
    Map<Integer, PrimaryRecoveryHandle> mRecoveryHandles;
    private AtomicReference<ServerState> mServerState= new AtomicReference(ServerState.Recovery);
    private Object mBusinessMutext = new Object();

    AtomicLong mTotTxTime = new AtomicLong(0);
    AtomicLong mTxCount = new AtomicLong(0);
    Long mTxPrintCount;
    volatile StandbyRecoveryCntx mStandbyRecoveryCntx;

    public static void main(String[] args) {
        TxServer txServer= new TxServer();
        txServer.parseParameters( args );
        txServer.runServer();
    }

    public void transactionCompleted( TxCntx pTxCntx, long pTxStartTimeNano ) {
        if (!pTxCntx.isReplay()) {
            long tNanoTime = System.nanoTime() - pTxStartTimeNano;
            //System.out.println("Transaction completed seqno: " +pTxCntx.getMessageSequenceNumber() + " nano-time: " + tNanoTime);
            mTotTxTime.addAndGet( tNanoTime );
            long txn = mTxCount.incrementAndGet();
            if ((txn % mTxPrintCount) == 0) {
                mLogger.info("Tx Statistics, tx count: " + mTxCount.get() + " average tx time: " + ((mTotTxTime.get() / 1000) / mTxCount.get()) + " usec");
            }
        }
    }

    private void runServer() {

        loadConfiguration();

        // Enable or disable transaction timestamp tracing
       if (!mConfiguration.get("timestamps").getAsBoolean()) {
           AuxTimestamp.disable();
       }

        mTxPrintCount = mConfiguration.get("tx_stat_print_interval_tx_count").getAsLong();

       // Initialize the TX logger
        mTxLogger = new TxLogger(mConfiguration.get("service").getAsJsonObject().get("tx_logging").getAsJsonObject());
        mLogger.info("Server message sequence number is set to " + mTxLogger.getServerMessageSeqno());
        mRecoveryHandles = Collections.synchronizedMap(new HashMap<>());

        createAndStartExecutor();
        mIpcController = new IpcController( mNodeId, mConfiguration );
        mIpcController.addIpcCallback(this);
        mQSController = new QuorumStateController(mNodeId, mConfiguration, mIpcController, mTxLogger);
        mQSController.syncStateWithQuorumServer(); // Wait for getting a role from quorum serve
        creatAndStartAssetController();



        // Both primary and standby needs to build last known business states from transaction logs
        long tLastKnownSeqno = recoverFromTxLogFile();


        // If being standby we need to sync up with primary
        if (!mQSController.isPrimary()) {
            mStandbyRecoveryCntx = new StandbyRecoveryCntx(tLastKnownSeqno);
            recoveryFromPrimary(mStandbyRecoveryCntx);
            synchronizingWithPrimary( mStandbyRecoveryCntx );
        } else {
            mServerState.set(ServerState.Synchronized);
            mLogger.info("Primary server is now synchronized");
        }


        createAndStartFE();
        mLogger.info( "Configuration loaded and server initialized" );
    }

    public void synchronizingWithPrimary( StandbyRecoveryCntx pRecoveryCntx ) {
        // The standby server has now catch up with the primary and is pretty much in sync.
        // Now the standby server has to synch in to the processing live flow. We need to collect replicated transaction while synchronizing with
        // the primary server txlog. After that we can apply buffered live transactions.
        mLogger.info("Start synchronizing with primary");
        pRecoveryCntx.reset();
        mServerState.set(ServerState.Synchronizing);
        // Now we can start to collect replicated transaction from the primary
        recoveryFromPrimary(mStandbyRecoveryCntx);
        synchronized (mStandbyRecoveryCntx) {
            mLogger.info("Standby synchronizing, recovery complete applying buffers (" + mStandbyRecoveryCntx.getBufferedMessages().size() + ")");
            // Apply buffered live transactions
            for (ToStandby tsb : mStandbyRecoveryCntx.getBufferedMessages()) {
                if (tsb.getSequenceNumber() > mStandbyRecoveryCntx.getCurrentSeqno()) {
                    queueInboundClientMessage(new TxCntx(tsb.getMessage(), tsb.getSequenceNumber()));
                }
            }
            mServerState.set(ServerState.Synchronized);
            mLogger.info("Standby server is now synchronized with primary (buffered messages: " +mStandbyRecoveryCntx.getBufferedMessages().size());
        }
    }


    public void recoveryFromPrimary( StandbyRecoveryCntx pRecoveryCntx ) {
        // Send our last known message sequence number and ask the primary server
        // to reply its messages sequence from that sequence number
        long tStartTime = System.currentTimeMillis();

        synchronized (pRecoveryCntx) {
            StandbyRecoveryRequest tRequest = new StandbyRecoveryRequest();
            tRequest.setLastKnownMessageSeqno(pRecoveryCntx.getLastKnownSeqno());
            tRequest.setHandleId(0);
            mIpcController.send(mQSController.getPrimaryNodeId(), tRequest);
            mLogger.info("Start recover message flow from primary (" + pRecoveryCntx.getLastKnownSeqno() + ")");
            try {
                pRecoveryCntx.wait();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        mLogger.info("Completed recover message flow from primary exec-time: " + (System.currentTimeMillis() - tStartTime) +
                " ms. Current msg Seqno: " + pRecoveryCntx.getCurrentSeqno() + " Msgs received: " + pRecoveryCntx.getMessagesReceived());
    }

    public void processRecoveryResponseFromPrimary( StandbyRecoveryResponse pPrimResponse ) {
        FEFactory tFactory = new FEFactory();

        mLogger.trace("Received recovery response from primary");
        if  (pPrimResponse.getMessageData() != null) {
            for (RecoveryData tData : pPrimResponse.getMessageData()) {
                MessageInterface tMsg = tFactory.createMessage(tData.getData());
                queueInboundClientMessage(new TxCntx(tMsg, tData.getMsgSeqno()));
                mStandbyRecoveryCntx.setCurrentSeqno(tData.getMsgSeqno());
                mStandbyRecoveryCntx.incrementMessagesReceived();
            }
        }
        if (!pPrimResponse.getEndOfData()) {
            mLogger.trace("Recover message flow from primary (" + mStandbyRecoveryCntx.getCurrentSeqno() + ")");
            StandbyRecoveryRequest tRequest = new StandbyRecoveryRequest();
            tRequest.setLastKnownMessageSeqno(mStandbyRecoveryCntx.getCurrentSeqno());
            tRequest.setHandleId( pPrimResponse.getHandleId());
            mIpcController.send(mQSController.getPrimaryNodeId(), tRequest);
        } else {
            synchronized (mStandbyRecoveryCntx) {
                mStandbyRecoveryCntx.notifyAll();
            }
        }
    }

    public void processRecoveryRequestFromStandby(IpcCntx pIpcCntxRequest) {
        PrimaryRecoveryHandle tRecoveryHandle = null;
        StandbyRecoveryRequest tToPrimaryRecoveryRequest = (StandbyRecoveryRequest) pIpcCntxRequest.getMessage();

        if (tToPrimaryRecoveryRequest.getHandleId() == 0) {
            tRecoveryHandle = new PrimaryRecoveryHandle(mTxLogger);
            mRecoveryHandles.put(tRecoveryHandle.getId(), tRecoveryHandle);

        } else {
            tRecoveryHandle = mRecoveryHandles.get(tToPrimaryRecoveryRequest.getHandleId());
            if (tRecoveryHandle == null) {
                throw new RuntimeException("No recovery handle");
            }
        }

        StandbyRecoveryResponse tResponse = tRecoveryHandle.recoverReplayAtPrimary(tToPrimaryRecoveryRequest);
        if (tResponse.getEndOfData()) {

            mRecoveryHandles.remove(tToPrimaryRecoveryRequest.getHandleId());
            if (tResponse.getMessageData().size() > 0) {
                long tLastSeqNo = tResponse.getMessageData().get( tResponse.getMessageData().size() - 1).getMsgSeqno();
                mLogger.info("Completed recover message flow to standby (" + tLastSeqNo + ")");
            } else {
                mLogger.info("Completed recover message flow to standby, no data in response, no data in response ");
            }
        }
        try {
            pIpcCntxRequest.send( tResponse );
        }
        catch( IOException e ) {
            mRecoveryHandles.remove(tToPrimaryRecoveryRequest.getHandleId());
            mLogger.error("Failed to send standby recovery response to standby, reason: " + e.getMessage());
        }
    }


    private long recoverFromTxLogFile() {
        long tStartTime = System.currentTimeMillis();
        long tRecordsReplayed = 0L;
        long tLastKnownSeqno = 0;

        FEFactory tMsgFactory = new FEFactory();
        String tTxLogfilePattern = mTxLogger.getLogFilePattern();

        if (tTxLogfilePattern.contains(TxLogger.SEQUENCE_DATETIME)) {
            tTxLogfilePattern = tTxLogfilePattern.replace(TxLogger.SEQUENCE_DATETIME, "*");
        } else {
            tTxLogfilePattern = tTxLogfilePattern.replace(TxLogger.SEQUENCE_SEQUENCE, "*");
        }
        TxlogReplayer tReplayer = mTxLogger.getReplayer(tTxLogfilePattern, TxlogReplayer.FORWARD);
        while( true ) {
            TxlogReplayRecord tRec = tReplayer.next();
            if (tRec == null) {break;}

            tLastKnownSeqno = tRec.getMsgSeqno();
            tRecordsReplayed++;
            MessageInterface tMsg = tMsgFactory.createMessage(tRec.getData());
            TxCntx txCntx = new TxCntx(tMsg);
            processClientMessage( txCntx );
        }
        mLogger.info("Replayed TX log files, records replayed: " + tRecordsReplayed + " time: " + (System.currentTimeMillis() - tStartTime) + " ms.");
        return tLastKnownSeqno;
    }

    private void creatAndStartAssetController() {
        mAssetController = new AssetController( mConfiguration, mQSController, mIpcController, mTxLogger );
    }

    @Override
    public long getServerMessageSeqno() {
        return mAssetController.getServerMessageSeqno();
    }

    private void createAndStartFE() {
        JsonObject jNodeConfig = getNodeConfiguration( mNodeId );
        JsonObject jFEConfig = jNodeConfig.get( "frontend_interface" ).getAsJsonObject();
        mFEController = new FEController(mNodeId, jFEConfig.get( "net_interface" ).getAsString(), jFEConfig.get( "tcpip_port" ).getAsInt(), this);
    }

    private void createAndStartExecutor() {
        int tThreadCount = mConfiguration.get( "executor_threads" ).getAsInt();
        mTxExecutor = new TxExecutor( this, tThreadCount );
    }

    private void parseParameters( String[] args ) {
        int i = 0;
        while( i < args.length ) {
            if( args[i].equals( "-config" ) ) {
                mConfigFilename = args[++i];
            }
            if( args[i].equals( "-id" ) ) {
                mNodeId = Integer.parseInt(args[++i]);
            }
            i++;
        }
        if (mNodeId == 0) {
            mLogger.error( "Server ID not specified as parameter." );
            System.exit( 1 );
        }
    }

    private JsonObject getNodeConfiguration( int pNodeId ) {
        JsonArray jNodes = mConfiguration.get( "nodes" ).getAsJsonArray();
        for (int i = 0; i < jNodes.size(); i++) {
            JsonObject jNode = jNodes.get(i).getAsJsonObject();
            if (jNode.get( "node_id" ).getAsInt() == pNodeId) {
                return jNode;
            }
        }
        throw new RuntimeException("Configuration definitions for node id " + pNodeId + " not found");
    }


    private void loadConfiguration() {
        try {
            mConfiguration = JsonParser.parseReader( new FileReader( mConfigFilename ) ).getAsJsonObject();
        } catch( Exception e ) {
            mLogger.error( "Unable to load configuration file: " + mConfigFilename );
            System.exit( 1 );
        }
    }

    private void txloggingAndReplication( TxCntx pTxCntx ) {
        UpdateMessage updmsg = (UpdateMessage) pTxCntx.getRequestMessage();
        if (updmsg instanceof UpdateMessage) {
            long tMessageSeqno = 0L;


            // If not a reply of a transaction, the transaction should be written to the primary or standby tx logger
            // For each message i a txlog file a message sequence number is assigned. The sequence number is consecutively incremented for each message logged.
            // Since multiple threads can execute in parallell for different assets but logged to the same txlog file we need to synchronize the access to the txlogger
            if (!pTxCntx.isReplay()) {
                    pTxCntx.addTimestamp("queue message to tx logger");
                    if (!pTxCntx.isPrimaryTx()) {
                        tMessageSeqno = pTxCntx.getMessageSequenceNumber(); // If being in stdby mode use the message sequence number from primary
                        mTxLogger.getWriter().queueMessage(updmsg.messageToBytes(), tMessageSeqno); // queue the message to the tx logger
                    } else {
                        tMessageSeqno = mTxLogger.getWriter().queueMessage(updmsg.messageToBytes()); // get an incremented message sequence number when being in primary mode
                        pTxCntx.setMessageSequenceNumber(tMessageSeqno);
                    }

                    // Replicate the message to the standby server if being in primary
                    if (pTxCntx.isPrimaryTx() && mQSController.isFailoverMode()) {
                        pTxCntx.addTimestamp("queue message to stdby");
                        mAssetController.publishMessageToStandby(pTxCntx);
                    }
                }
            }
    }

    public void queueInboundClientMessage(TxCntx pTxCntx) {
            // entry point for business transactions transaction
            // we want tx messages to be replicated to the standby in same order as tx messages are queues to txlogger
            // that should give the tx order in the primary/standy to be the
            synchronized( mBusinessMutext ) {
                txloggingAndReplication( pTxCntx );
                mTxExecutor.queueRequest(pTxCntx);
            }
            pTxCntx.addTimestamp("queued TxCntx for business processing");
    }

    public void processClientMessage(TxCntx pTxCntx)
    {
        mAssetController.processClientMessage( pTxCntx );
    }

    @Override
    public void onMessage(IpcCntx pIpcCntx) {
        // FromStandy TX replication confirmation
        if (pIpcCntx.getMessage().getMessageId() == FromStandby.MESSAGE_ID) {
            mAssetController.fromStandby((FromStandby) pIpcCntx.getMessage());
        }

        // StandbyRecoveryResponse primary replay response to standby part of standby initial synchronization
        if (pIpcCntx.getMessage().getMessageId() == StandbyRecoveryResponse.MESSAGE_ID) {
            processRecoveryResponseFromPrimary((StandbyRecoveryResponse) pIpcCntx.getMessage());
        }
        // StandbyRecoveryRequest , request from standby to primary to replay some of it txlog
        if (pIpcCntx.getMessage().getMessageId() == StandbyRecoveryRequest.MESSAGE_ID) {
            processRecoveryRequestFromStandby(pIpcCntx);
        }


        // The primary replicates "ToStandby" message to the standby. These messages are ignored
        // unless the server is in a synchronized state
        if (pIpcCntx.getMessage().getMessageId() == ToStandby.MESSAGE_ID) {
            if (mQSController.isPrimary()) {
                mLogger.error("Should never receive a ToStanby message when being a Primary");
                return;
            }
            // This is a bit obscure, when the main thread needs to be synchronized with the
            // incomming tcp/ip read events. This will only happen when being in the
            // "Synchronizing" mode.
            if (mServerState.get() == ServerState.Synchronized) {
                processBusinessMessage(pIpcCntx);
            } else if (mServerState.get() == ServerState.Synchronizing) {
                synchronized (mStandbyRecoveryCntx) {
                    if (mServerState.get() == ServerState.Synchronizing) {
                        mStandbyRecoveryCntx.addToStandbyMessage((ToStandby) pIpcCntx.getMessage());
                    } else if (mServerState.get() != ServerState.Synchronized) {
                        processBusinessMessage(pIpcCntx);
                    }
                }
            }
        }
    }

    private void processBusinessMessage( IpcCntx pIpcCntx ) {
        ToStandby toStandby = (ToStandby) pIpcCntx.getMessage();
        FromStandby fromStandby = new FromStandby();

        fromStandby.setSequenceNumber(toStandby.getSequenceNumber());
        fromStandby.setAssetId(((RequestMessage) toStandby.getMessage()).getAssetId());
        try {
            pIpcCntx.send(fromStandby);
        } catch (IOException e) {
            mLogger.error("Failed to send stdby reply to primary, reason: " + e.getMessage());
        }

        TxCntx txCntx = new TxCntx(toStandby.getMessage(), toStandby.getSequenceNumber());
        queueInboundClientMessage(txCntx);
    }


    @Override
    public void onConnect(IpcNode pIpcNode)
    {
        // No action by design
    }

    @Override
    public void onDisconnect(IpcNode pIpcNode, boolean pHardDisconnect) {
        // No action by design
    }
}
