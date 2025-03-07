package com.hoddmimes.txtest.quorum;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hoddmimes.distributor.messaging.MessageInterface;
import com.hoddmimes.txtest.aux.net.TcpServer;
import com.hoddmimes.txtest.aux.net.TcpServerCallbackIf;
import com.hoddmimes.txtest.aux.net.TcpThread;
import com.hoddmimes.txtest.aux.net.TcpThreadCallbackIf;
import com.hoddmimes.txtest.generated.ipc.messages.IPCFactory;
import com.hoddmimes.txtest.generated.ipc.messages.QuorumHeartbeat;
import com.hoddmimes.txtest.generated.ipc.messages.QuorumVoteRequest;
import com.hoddmimes.txtest.generated.ipc.messages.QuorumVoteResponse;
import com.hoddmimes.txtest.server.ServerState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class QuorumServer extends Thread implements TcpServerCallbackIf, TcpThreadCallbackIf
{
    private JsonObject jConfiguration = null;
    private JsonObject jQuorum = null;
    private TcpServer mTcpServer;
    private Logger mLogger;
    private HashMap<String, QuorumService> mServices;

    private long mQuorumServerStartTime;


    private long mHearbeatTimeoutMs;
    private boolean mDisconnectIsFailure;
    private IPCFactory mMsgFactory;


    public static void main(String[] args) {
        final QuorumServer qs = new QuorumServer();
        qs.parseArguments( args );
        qs.initialize();
        qs.declareAndRun();
        qs.start();
    }

    QuorumServer() {
        mLogger = LogManager.getLogger(this.getClass().getSimpleName());
        mServices = new HashMap<>();
        mMsgFactory = new IPCFactory();
    }

    private void initialize() {
        mQuorumServerStartTime = System.currentTimeMillis();

        jQuorum = jConfiguration.get("quorum_server").getAsJsonObject();
        long tVoteInterval = jQuorum.get("vote_interval_sec").getAsLong() * 1000L;

        JsonObject jService = jConfiguration.get("service").getAsJsonObject();
        String tServiceName = jService.get("name").getAsString();
        QuorumService qs = new QuorumService( tServiceName, tVoteInterval, mLogger );
        mServices.put( tServiceName, qs );

        // Parse out preferred primary node
        JsonObject jPreferedPrimary = jService.get("preferred_primary").getAsJsonObject();
        int tNodeId = jPreferedPrimary.get("node_id").getAsInt();
        JsonObject jNode = getConfigNode( tNodeId );
        qs.addNode( tNodeId, jNode.get("ip").getAsString(), jNode.get("tcp_port").getAsInt(), true);

        // Parse out preferred standby node
        JsonObject jPreferedStdby = jService.get("preferred_standby").getAsJsonObject();
        tNodeId = jPreferedStdby.get("node_id").getAsInt();
        jNode = getConfigNode( tNodeId );
        qs.addNode( tNodeId, jNode.get("ip").getAsString(), jNode.get("tcp_port").getAsInt(), false);
        mLogger.info("Initial defined service");

        JsonObject jIPC = jConfiguration.get("ipc_connections").getAsJsonObject();
        mHearbeatTimeoutMs = jIPC.get("hb_intervals_sec").getAsLong() * 1000L * jIPC.get("max_missed_hb").getAsLong();
        mDisconnectIsFailure = jIPC.get("disconnect_is_failure").getAsBoolean();



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
        JsonObject jNode = getConfigNode( tNodeId );

        int tPort = jNode.get("tcp_port").getAsInt();
        String tIp = jNode.get("ip").getAsString();

        try {
            mTcpServer = new TcpServer( this );
            mTcpServer.declareServer( tIp, tPort);
            mLogger.info("Starting quorum server on interface " + tIp + " port " + tPort );
        }
        catch( IOException e) {
            throw new RuntimeException("Failed to declare and run server on interface " + tIp + " port " + tPort);
        }
    }

    synchronized private void processHearbeat( QuorumHeartbeat pMessage ) {
        QuorumService tService = mServices.get( pMessage.getService() );
        QuorumService.QNode tNode = tService.getNode( pMessage.getNodeId());
        tNode.heartbeat( pMessage );

    }

    synchronized private QuorumVoteResponse returnVoteResponse( int pNodeId, ServerState pState, String pReason)  {
        QuorumVoteResponse tResponse = new QuorumVoteResponse();
        tResponse.setNodeId(pNodeId);
        tResponse.setServerState(pState.getValue());
        tResponse.setReason(pReason);
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
        QuorumService tService = mServices.get(pMessage.getService());
        // Case 2
        // Check if we have a know state for the primary/standby nodes if so we can decide
        ServerState tState = tService.findNodeState( pMessage.getNodeId());
        if (tState != ServerState.UNKNOWN) {
            mLogger.trace("VOTE: primary/standy can be decided based upon quorum server knowing the states \n" + pMessage.toString());
            return returnVoteResponse( pMessage.getNodeId(), tState, "VOTE: State set from previous session by \"findNodeState\" ");
        }

        // Check if we are still in the scout out phase, if so ignore the vote request
        long tScoutDelay = jQuorum.get("startup_scout_initial_delay_sec").getAsLong() * 1000L;
        if ((System.currentTimeMillis() - mQuorumServerStartTime) < tScoutDelay) {
            mLogger.trace("VOTE: Still in the scout out phase, ignoring vote request from node id: \n" + pMessage.toString());
            return null;
        }

        // Save the vote request for a later decision
        if (tService.addVoteRequestAndCheckIfExpired( pMessage )) {
            // we will likely not be able to determine primary/standby roles since
            // all service nodes are not present withing the voting interval
            mLogger.trace("VOTE: Vote period has now expired, will not be able to determine primary/standby roles, ignoring vote request \n" + pMessage.toString());
            return null;
        }

        // Can we take a decision based upon available vote requests from service nodes?
        if (tService.validateVoteRequests()) {
            mLogger.trace("VOTE: Can now determine primary/standby roles having sufficient vote requests\n" + pMessage.toString());
            QuorumService.QNode tNode = tService.getNode(pMessage.getNodeId());
            return returnVoteResponse(pMessage.getNodeId(), tNode.mState, "VOTE: State set after completed voting process");
        }

         mLogger.trace("VOTE: Can not determine primary/standby roles, still waiting for servers to connect and send vote requests\n" + pMessage.toString());
         return null;

    }


    synchronized void processMessage( TcpThread pClient, MessageInterface pMessage ) {
        switch ( pMessage.getMessageId()) {
            case QuorumHeartbeat.MESSAGE_ID:
                processHearbeat(((QuorumHeartbeat) pMessage));
                return;
            case QuorumVoteRequest.MESSAGE_ID:
                QuorumVoteResponse tResponse = processVoteRequest( (QuorumVoteRequest) pMessage);
                if (tResponse != null) {
                    try {
                        pClient.send( tResponse.messageToBytes());
                    }
                    catch( IOException e) {
                        e.printStackTrace();
                        pClient.close();
                    }
                }
                return;
        }
        mLogger.warn("Unknown server message " + pMessage.getMessageName());
    }

    void nodeDisconnected( QuorumService pService, QuorumService.QNode pNode, boolean pHardDisconnect )
    {
        mLogger.warn("Node id: " + pNode.mId +  " disconnected, state: " + pNode.mState.toString() + " hard-disconnect: " + pHardDisconnect);
        pNode.isConnected = false;

    }

    synchronized void checkHeartbeatTimeout() {
        long tNow = System.currentTimeMillis();

        for (QuorumService tService : mServices.values()) {
            for (QuorumService.QNode tNode : tService.mNodes) {
                if (tNow - tNode.mLatestHeartbeat > mHearbeatTimeoutMs) {
                    nodeDisconnected( tService, tNode, false);
                }
            }
        }
    }

    public void run()
    {
        while( true ) {
            try { Thread.sleep( 1000L); }
            catch( InterruptedException e) {}

            checkHeartbeatTimeout();

        }
    }

    @Override
    public void tcpInboundConnection(TcpThread pThread) {
        mLogger.info( "Inbound connection from " + pThread.getRemoteAddress());

        pThread.setCallback( this );
        pThread.start();
    }

    @Override
    public void tcpMessageRead(TcpThread pThread, byte[] pBuffer)
    {
        MessageInterface tMessage = mMsgFactory.createMessage( pBuffer );
        processMessage( pThread, tMessage );
    }

    @Override
    public void tcpErrorEvent(TcpThread pThread, IOException pException) {
        String tIpAddress = pThread.getRemoteAddress();
        mLogger.warn("Node " + tIpAddress + " disconnected, reason: " + pException.getMessage());

        synchronized ( this ) {
            for (QuorumService tService : mServices.values()) {
                for (QuorumService.QNode tNode : tService.mNodes) {
                    if (tNode.mIP.equals(tIpAddress)) {
                        if (mDisconnectIsFailure) {
                            nodeDisconnected(tService, tNode, true);
                        }
                    }
                }
            }
        }
    }
}
