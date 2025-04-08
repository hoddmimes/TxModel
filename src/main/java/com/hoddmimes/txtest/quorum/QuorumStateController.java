package com.hoddmimes.txtest.quorum;

import com.google.gson.JsonObject;
import com.hoddmimes.msgcompiler.messaging.MessageInterface;
import com.hoddmimes.txtest.aux.ipc.IpcCallbacks;
import com.hoddmimes.txtest.aux.ipc.IpcCntx;
import com.hoddmimes.txtest.aux.ipc.IpcController;
import com.hoddmimes.txtest.aux.ipc.IpcNode;
import com.hoddmimes.txtest.generated.ipc.messages.QuorumStatusHeartbeat;
import com.hoddmimes.txtest.generated.ipc.messages.QuorumVoteRequest;
import com.hoddmimes.txtest.generated.ipc.messages.QuorumVoteResponse;
import com.hoddmimes.txtest.server.ServerMessageSeqnoInterface;
import com.hoddmimes.txtest.server.ServerRole;
import com.hoddmimes.txtest.server.TxServerIf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;


public class QuorumStateController implements IpcCallbacks
{
    public enum Mode {IGNORE, FAILOVER };

    private  Mode mMode;
    private QuorumHeartbeatPublisher mPublisher;
    private Map<Integer, QuorumNode> mQuorumNodes;
    private ServerMessageSeqnoInterface mSeqnoIf;
    private int mMyNodeId;
    private int mQuorumNodeId;
    private IpcController mIpc;
    private Logger mLogger;
    private QuorumNode mMyQuorumNode;

    public QuorumStateController( TxServerIf pTxServerIf ) {
        int tNodeId;

        mLogger = LogManager.getLogger(QuorumStateController.class);
        mMode = Mode.valueOf(pTxServerIf.getConfiguration().get("quorum_server").getAsJsonObject().get("mode").getAsString());

        if (mMode == Mode.IGNORE) {
            return;
        }

        mSeqnoIf = pTxServerIf.getMessageSequenceNumberIf();
        mIpc = pTxServerIf.getIpcController();
        mMyNodeId = pTxServerIf.getNodeId();
        mQuorumNodeId = pTxServerIf.getConfiguration().get("quorum_server").getAsJsonObject().get("node_id").getAsInt();
        JsonObject jService = pTxServerIf.getConfiguration().get("service").getAsJsonObject();



        mQuorumNodes = new HashMap<>();
        tNodeId = jService.get("preferred_primary").getAsJsonObject().get("node_id").getAsInt();
        mQuorumNodes.put( tNodeId, new QuorumNode(tNodeId, true));
        tNodeId = jService.get("preferred_standby").getAsJsonObject().get("node_id").getAsInt();
        mQuorumNodes.put( tNodeId, new QuorumNode(tNodeId, false));

        mIpc.addIpcCallback(this);

        for(QuorumNode qn : mQuorumNodes.values()) {
            qn.setConnected(mIpc.isNodeConnected( qn.getNodeId()));
        }
    }



    public boolean isPrimary() {
        return (mMode == Mode.IGNORE) || (mMyQuorumNode.getRole() == ServerRole.PRIMARY);
    }

    public boolean isFailoverMode() {
        return (mMode == Mode.FAILOVER);
    }

    public int getPrimaryNodeId() {
        for( QuorumNode qn : mQuorumNodes.values() ) {
            if (qn.getRole() == ServerRole.PRIMARY) {
                return qn.mNodeId;
            }
        }
        throw new RuntimeException("Standby node id not found");
    }

    public int getStandByNodeId() {
        for( QuorumNode qn : mQuorumNodes.values() ) {
            if (qn.getRole() != ServerRole.PRIMARY) {
                return qn.mNodeId;
            }
        }
        throw new RuntimeException("Standby node id not found");
    }

    public void syncStateWithQuorumServer() {

        if (mMode == Mode.IGNORE) {
            mLogger.info("Not started in failover mode, sync with quorum,primary,standby is ignored");
            return;
        }

        mMyQuorumNode = mQuorumNodes.get( mMyNodeId );

        while(  mMyQuorumNode.getRole() == ServerRole.UNKNOWN) {
            mLogger.info("Synchronizing with Quorum server...");
            QuorumVoteRequest vr = new QuorumVoteRequest();
            vr.setNodeId( mMyNodeId );
            vr.setCurrentSeqno( mSeqnoIf.getServerMessageSeqno() );
            vr.setCurrentRole(mMyQuorumNode.getRole().getValue());
            vr.setWannabeRole(mMyQuorumNode.getRole().getValue());
            mLogger.trace("Sending vote-request " + vr);

            mIpc.send(mQuorumNodeId, vr);

            try { Thread.sleep(5000L); }
            catch(InterruptedException e) {}
        }

        mLogger.info("Server state is now: " + mMyQuorumNode.getRole().toString());
        mPublisher = new QuorumHeartbeatPublisher( mQuorumNodes.get( mMyNodeId));
        mPublisher.start();
    }

    @Override
    public void onMessage(IpcCntx pIpcCntx)
    {
        MessageInterface tMessage = pIpcCntx.getMessage();
        if (tMessage.getMessageId() == QuorumVoteResponse.MESSAGE_ID) {
            QuorumVoteResponse tVoteRsp = (QuorumVoteResponse) tMessage;
            mLogger.info( "Received vote response, state: " + ServerRole.valueOf(tVoteRsp.getRole()) +
                    " (my-node-id: " + mMyQuorumNode.getNodeId() + " primary-node-id: " + tVoteRsp.getPrimaryNodeId() + " standby-node-id: " + tVoteRsp.getStandbyNodeId() + " )");

            mMyQuorumNode.setState(ServerRole.valueOf(tVoteRsp.getRole()));
            for( QuorumNode node : mQuorumNodes.values()) {
                if (node.getNodeId() == tVoteRsp.getPrimaryNodeId()) {
                   if (node.getRole() != ServerRole.PRIMARY) {
                     node.setState(ServerRole.PRIMARY);
                   }
                } else if (node.getNodeId() == tVoteRsp.getStandbyNodeId()) {
                    if (node.getRole() != ServerRole.STANDBY) {
                        node.setState(ServerRole.STANDBY);
                    }
                }
            }
        }
        if (tMessage.getMessageId() == QuorumStatusHeartbeat.MESSAGE_ID) {
            System.out.println( tMessage );
        }
    }

    @Override
    public void onConnect(IpcNode pIpcNode) {
        QuorumNode quorumNode = mQuorumNodes.get( pIpcNode.getNodeId());
        if (quorumNode != null) {
            quorumNode.setConnected(true);
            mLogger.info("[CONNECTED] Successfully connected to node " + pIpcNode);
        }
    }

    @Override
    public void onDisconnect(IpcNode pIpcNode, boolean pHardDisconnect) {


        QuorumNode quorumNode = mQuorumNodes.get( pIpcNode.getNodeId());
        if (quorumNode != null) {
            quorumNode.setConnected(false);
            mLogger.info("[DISCONNECTED] node " + pIpcNode);
            QuorumNode tMyQuorumNode = mQuorumNodes.get(mMyNodeId);
            // If Primare failes and this instance is standby then it's time to step up as primary
            if ((pIpcNode.getNodeId() != mMyNodeId) && (tMyQuorumNode.getRole() == ServerRole.STANDBY)) {
                QuorumVoteRequest vr = new QuorumVoteRequest();
                vr.setNodeId(mMyNodeId);
                vr.setCurrentSeqno(mSeqnoIf.getServerMessageSeqno());
                vr.setCurrentRole(tMyQuorumNode.getRole().getValue());
                vr.setWannabeRole(ServerRole.PRIMARY.getValue());
                mLogger.trace("Sending vote-request to become PRIMARY" + vr);
                mIpc.send(mQuorumNodeId, vr);
            }
        }
    }

    class QuorumHeartbeatPublisher extends Thread {
        QuorumNode mMyQuorumNode;

        QuorumHeartbeatPublisher( QuorumNode pMyQuorumNode ) {
            mMyQuorumNode = pMyQuorumNode;
        }

        private void sendQuorumHeartbeat() {
            QuorumStatusHeartbeat qhb = new QuorumStatusHeartbeat();
            qhb.setCurrentSeqno( mSeqnoIf.getServerMessageSeqno());
            qhb.setNodeId( mMyNodeId );
            qhb.setServerRole( mMyQuorumNode.getRole().getValue());

            if (mIpc.send(mQuorumNodeId, qhb)) {
                mLogger.trace("publishing quorum heartbeat " + qhb );
            }

        }

        public void run() {
            while( true ) {
                try { Thread.sleep( 1000L); }
                catch( InterruptedException e) {}
                sendQuorumHeartbeat();
            }
        }


    }
}
