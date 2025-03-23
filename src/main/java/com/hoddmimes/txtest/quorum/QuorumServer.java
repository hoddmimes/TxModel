package com.hoddmimes.txtest.quorum;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hoddmimes.txtest.aux.ipc.IpcCallbacks;
import com.hoddmimes.txtest.aux.ipc.IpcCntx;
import com.hoddmimes.txtest.aux.ipc.IpcController;
import com.hoddmimes.txtest.aux.ipc.IpcNode;
import com.hoddmimes.txtest.generated.ipc.messages.IPCFactory;
import com.hoddmimes.txtest.generated.ipc.messages.QuorumStatusHeartbeat;
import com.hoddmimes.txtest.generated.ipc.messages.QuorumVoteRequest;
import com.hoddmimes.txtest.generated.ipc.messages.QuorumVoteResponse;
import com.hoddmimes.txtest.server.ServerRole;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileReader;
import java.io.IOException;

public class QuorumServer implements IpcCallbacks
{
    private JsonObject jConfiguration = null;
    private JsonObject jQuorumConfiguration = null;
    private IpcController mIPC;
    private Logger mLogger;
    private QuorumService mService;

    private long mQuorumServerStartTime;
    private IPCFactory mMsgFactory;



    public static void main(String[] args) {
        final QuorumServer qs = new QuorumServer();
        qs.parseArguments( args );
        qs.initialize();
        qs.declareAndRun();
        while( true ) {
            try { Thread.sleep(1000L); }
            catch( InterruptedException e) {}
        }
    }

    QuorumServer() {
        mQuorumServerStartTime = System.currentTimeMillis();
        mLogger = LogManager.getLogger(QuorumServer.class);
        mLogger.trace("Starting Quorum Server");
        mMsgFactory = new IPCFactory();
    }



    private void initialize() {

        jQuorumConfiguration = jConfiguration.get("quorum_server").getAsJsonObject();
        long tVoteInterval = jQuorumConfiguration.get("voting_process_timeout_sec").getAsLong() * 1000L;

        JsonObject jService = jConfiguration.get("service").getAsJsonObject();
        mService = new QuorumService(tVoteInterval, mLogger );

        // Parse out preferred primary node
        JsonObject jPreferedPrimary = jService.get("preferred_primary").getAsJsonObject();
        int tNodeId = jPreferedPrimary.get("node_id").getAsInt();
        JsonObject jNode = getConfigNode( tNodeId );
        mService.addNode( tNodeId, jNode.get("ip").getAsString(), jNode.get("tcp_port").getAsInt(), true);

        // Parse out preferred standby node
        JsonObject jPreferedStdby = jService.get("preferred_standby").getAsJsonObject();
        tNodeId = jPreferedStdby.get("node_id").getAsInt();
        jNode = getConfigNode( tNodeId );
        mService.addNode( tNodeId, jNode.get("ip").getAsString(), jNode.get("tcp_port").getAsInt(), false);
        mLogger.info("Initial defined service");
    }

    private JsonObject getConfigNode( int pNodeId ) {
        JsonArray jNodeArray = jConfiguration.get("nodes").getAsJsonArray();
        for (int i = 0; i < jNodeArray.size(); i++) {
            JsonObject jNode = jNodeArray.get(i).getAsJsonObject();
            if (jNode.get("node_id").getAsInt() == pNodeId) {
                return jNode;
            }
        }
        throw new RuntimeException("Configuration definitions for node id " + pNodeId + " not found");
    }

    private void parseArguments( String[] args ) {
        int i = 0;
        while( i < args.length ) {
            if (args[i].equals("-config")) {
                try {
                    jConfiguration = JsonParser.parseReader(new FileReader(args[++i])).getAsJsonObject();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            i++;
        }
        if (jConfiguration == null) {
            throw new RuntimeException("Configuration not specified \"-config\"");
        }
    }

    private void declareAndRun() {
        int tNodeId = jConfiguration.get("quorum_server").getAsJsonObject().get("node_id").getAsInt();

        mIPC = new IpcController( tNodeId, jConfiguration );
        mIPC.addIpcCallback(this);
    }



    private void processQuorumStatusBdx( QuorumStatusHeartbeat pMessage ) {
        QuorumNode tNode = mService.getNode( pMessage.getNodeId());
        tNode.statusHeartbeat( pMessage );
    }

    synchronized private QuorumVoteResponse returnVoteResponse(int pNodeId, ServerRole pRole, String pReason, int pPrimaryNodeId)  {
        QuorumVoteResponse tResponse = new QuorumVoteResponse();
        tResponse.setNodeId(pNodeId);
        tResponse.setRole(pRole.getValue());
        tResponse.setReason(pReason);
        tResponse.setPrimaryNodeId( pPrimaryNodeId );
        return tResponse;
    }

    /** ===========================================================================
     * When server client has started it will establish a connection to the quorum server
     * and submit a vote request. The connection is established due to
     * 1) The Quorum server has been started.
     * 2) One or all service nodes has been started.
     * 3) All nodes, quorum and service nodes has been restarted
     *
     * Scenario 1
     * ------------
     * In this case the service nodes could have the roles estblished. They will publish Quorum heartbeat messages with their states.
     * The quorum server will then just set the service nodes states based upon what they publish. If all service nodes are up and
     * have roles established, no vote request will be sent.
     *
     * Scenario 2
     * ___________
     * In this case the quorum server is restarted and the service nodes last known states from when previous session is known.
     * The service nodes should then be assigned the roles from the last known session.
     *
     * Scenario 3
     * -----------
     * If the whole system is restarted, inclusive the quorum server. No knowledge about service states are known. If the system has been restarted and
     * all service nodes connects. The system with the highest known (txlog) sequence number will be selected as primary. If tied i.e.
     * all service nodes has the same sequence number (e.g. 0  with a fresh startup). The "preferred-primary" will be selected as primary.
     *
     * The challenging is when all nodes are restarted but one or more service nodes are absent. In this case it's not possible to
     * determine primary node. Needs to be handler manually.
     *
     * @param pMessage (vote request)
     */
     QuorumVoteResponse processVoteRequest( QuorumVoteRequest pMessage ) {
         // The most intresting case is when the primary fails and the secondary would like to become the new primary
         QuorumNode quorumNode = mService.getNode(pMessage.getNodeId()); // get requester nodeid

         if ((quorumNode.getRole() == ServerRole.STANDBY) && (pMessage.getWannabeRole() == ServerRole.PRIMARY.getValue())) {
             QuorumNode primaryNode = mService.getPrimary();
             if ((primaryNode != null) && (!primaryNode.isIsConnected())) {
                 mLogger.trace("VOTE: primary/standy failover !!! \n" + pMessage.toString());
                 quorumNode.setState( ServerRole.PRIMARY );
                 primaryNode.setState( ServerRole.STANDBY);
                 mLogger.trace("VOTE: primary/standy failover !!!" + pMessage.toString());
                 mLogger.trace("VOTE: SET STATE node " + quorumNode );
                 mLogger.trace("VOTE: SET STATE node " + primaryNode );
                 return returnVoteResponse( pMessage.getNodeId(), quorumNode.getRole(), "VOTE: State changed stdby/primary failover", quorumNode.getNodeId());
             }
         }

        // Case 2
        // Check if we have a know state for the primary/standby nodes if so we can decide
         quorumNode = mService.getNode(pMessage.getNodeId());
         if (quorumNode.isIsConnected() && quorumNode.getRole() != ServerRole.UNKNOWN) {
             QuorumNode tPrimeryQuorumNode = mService.getPrimary();
             mLogger.trace("VOTE: primary/standy can be decided based upon quorum server knowing the states \n" + pMessage.toString());
             return returnVoteResponse( pMessage.getNodeId(), quorumNode.getRole(), "VOTE: State set from previous session by \"findNodeState\" ", tPrimeryQuorumNode.getNodeId());
         }

        // Check if we are still in the scout out phase, if so ignore the vote request

        long tScoutDelay = jQuorumConfiguration.get("startup_scout_initial_delay_sec").getAsLong() * 1000L;
        if ((System.currentTimeMillis() - mQuorumServerStartTime) < tScoutDelay) {
            mLogger.trace("VOTE: Still in the scout out phase, ignoring vote request from node id: \n" + pMessage.toString());
            return null;
        }

        // Save the vote request for a later decision
        if (mService.addVoteRequestAndCheckIfExpired( pMessage )) {
            // we will likely not be able to determine primary/standby roles since
            // all service nodes are not present withing the voting interval
            mLogger.trace("VOTE: Vote period has now expired, will not be able to determine primary/standby roles, ignoring vote request \n" + pMessage.toString());
            return null;
        }

        // Can we take a decision based upon available vote requests from service nodes?
        if (mService.validateVoteRequests()) {
            mLogger.trace("VOTE: Can now determine primary/standby roles having sufficient vote requests\n" + pMessage.toString());
            QuorumNode tNode = mService.getNode(pMessage.getNodeId());
            QuorumNode tPrimaryNode = mService.getPrimary();
            return returnVoteResponse(pMessage.getNodeId(), tNode.getRole(), "VOTE: State set after completed voting process", tPrimaryNode.getNodeId());
        }

         mLogger.trace("VOTE: Can not determine primary/standby roles, still waiting for servers to connect and send vote requests\n" + pMessage.toString());
         return null;

    }


    @Override
    public synchronized void onMessage(IpcCntx pIpcCntx) {
        switch ( pIpcCntx.getMessage().getMessageId()) {
            case QuorumStatusHeartbeat.MESSAGE_ID:
                processQuorumStatusBdx((QuorumStatusHeartbeat)  pIpcCntx.getMessage());
                return;
            case QuorumVoteRequest.MESSAGE_ID:
                QuorumVoteResponse tResponse = processVoteRequest( (QuorumVoteRequest) pIpcCntx.getMessage());
                if (tResponse != null) {
                    try {pIpcCntx.send( tResponse );}
                    catch( IOException e) {
                        mLogger.error("Failed to send vote response to node id: " + tResponse.getNodeId(), e);
                    }
                }
                return;
        }
        mLogger.warn("Unknown server message " + pIpcCntx.getMessage().getMessageName());
    }

    @Override
    public synchronized void onConnect( IpcNode pIpcNode) {
        mLogger.info( "Inbound connection from {}", pIpcNode.toString());

        QuorumNode quorumNode = mService.getNode( pIpcNode.getNodeId());
        if ((quorumNode != null) && (!quorumNode.isIsConnected())) {
             quorumNode.setConnected( true );
        }
    }

    @Override
    public void onDisconnect(IpcNode pIpcNode, boolean pHardReset ) {
        QuorumNode quorumNode = mService.getNode( pIpcNode.getNodeId());
        quorumNode.setConnected( false );
        mLogger.info( "Node id: {} is now DISCONNECTED", pIpcNode.getNodeId());
    }

}
