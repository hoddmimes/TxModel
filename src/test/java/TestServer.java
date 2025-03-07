import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hoddmimes.distributor.messaging.MessageInterface;
import com.hoddmimes.txtest.aux.net.TcpClient;
import com.hoddmimes.txtest.aux.net.TcpThread;
import com.hoddmimes.txtest.aux.net.TcpThreadCallbackIf;
import com.hoddmimes.txtest.generated.ipc.messages.IPCFactory;
import com.hoddmimes.txtest.generated.ipc.messages.QuorumHeartbeat;
import com.hoddmimes.txtest.generated.ipc.messages.QuorumVoteRequest;
import com.hoddmimes.txtest.generated.ipc.messages.QuorumVoteResponse;
import com.hoddmimes.txtest.server.ServerState;

import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicLong;



public class TestServer extends Thread implements TcpThreadCallbackIf
{
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    AtomicLong mSeqno = new AtomicLong( 0L );
    JsonObject jConfiguration;
    TcpThread mClient;
    int mNodeId = 0;
    ServerState mState = ServerState.UNKNOWN;

    public static void main(String[] args) {
        TestServer ts = new TestServer();
        ts.parseArguments( args );
        ts.loadConfiguration();
        ts.connectToQuorumServer();
        ts.syncWithQuorumServer();
        ts.start();
    }


    private void parseArguments( String[] args ) {
        int i = 0;
        while( i < args.length) {
            if (args[i].equals("-id")) {
                mNodeId = Integer.parseInt(args[++i]);
            }
            if (args[i].equals("-seqno")) {
                mSeqno.set(Long.parseLong(args[++i]));
            }
            i++;
        }
        if (mNodeId == 0) {
            throw new RuntimeException("Server ID not specified as parameter");
        }
    }

    private void log( String pLogMsg ) {
        System.out.println( sdf.format( System.currentTimeMillis()) + " " + pLogMsg);
        System.out.flush();
    }

    private void connectToQuorumServer() {
        int tNodeId = jConfiguration.get("quorum_server").getAsJsonObject().get("node_id").getAsInt();
        JsonArray jNodeArr = jConfiguration.get("nodes").getAsJsonArray();
        JsonObject jQuoromNode = null;

        for (int i = 0; i < jNodeArr.size(); i++) {
            JsonObject jNode = jNodeArr.get(i).getAsJsonObject();
            if (jNode.get("node_id").getAsInt() == tNodeId) {
                jQuoromNode = jNode;
            }
        }

        if (jQuoromNode == null) {
            throw new RuntimeException("Configuration definitions for node id " + tNodeId + " not found");
        }

        boolean tConnected = false;

        while(!tConnected) {
            try {
                mClient = TcpClient.connect( jQuoromNode.get("ip").getAsString(), jQuoromNode.get("tcp_port").getAsInt(), this);
                tConnected = true;
            }
            catch( IOException e) {
                log("ERROR: Failed to connect to quorum server,reason: " + e.getMessage());
                try { Thread.sleep(5000L);}
                catch(InterruptedException ei) {}
            }
        }
    }

    private void loadConfiguration() {
        try {
            jConfiguration = JsonParser.parseReader( new FileReader( "TxServer.json" ) ).getAsJsonObject();
        } catch( Exception e ) {
            log( "ERROR: Unable to load configuration file");
            System.exit( 1 );
        }
    }

    private void syncWithQuorumServer() {
        while( mState == ServerState.UNKNOWN) {
            QuorumVoteRequest vr = new QuorumVoteRequest();
            vr.setService("test_tx");
            vr.setNodeId( mNodeId );
            vr.setCurrentSeqno( mSeqno.get());
            vr.setRequestedServerState( mState.getValue());
            log("Sending vote request:  " + vr.toString());
            try {mClient.send( vr.messageToBytes());}
            catch(IOException e) {e.printStackTrace();}

            try { Thread.sleep(5000L); }
            catch(InterruptedException e) {}
        }
    }

    private void sendQuorumHeartbeat() {
        QuorumHeartbeat hb = new QuorumHeartbeat();
        hb.setMsgSeqno( mSeqno.get());
        hb.setNodeId( mNodeId );
        hb.setService("test_tx");
        hb.setServerState( mState.getValue());

        try {
            mClient.send( hb.messageToBytes());
        }
        catch(IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        while( true ) {
            try { Thread.sleep( 1000L); }
            catch( InterruptedException e) {}
            mSeqno.incrementAndGet();
            sendQuorumHeartbeat();
        }
    }

    @Override
    public void tcpMessageRead(TcpThread pThread, byte[] pBuffer) {
        IPCFactory mFactory = new IPCFactory();
        MessageInterface msg = mFactory.createMessage(pBuffer);
        switch (msg.getMessageId()) {
            case QuorumVoteResponse.MESSAGE_ID:
                processVoteResponse(((QuorumVoteResponse) msg));
                return;
        }
    }

    private void processVoteResponse( QuorumVoteResponse msg) {
        log( "Received vote response, state: " + ServerState.valueOf(msg.getServerState()).toString() );
        mState = ServerState.valueOf(msg.getServerState());
    }

    @Override
    public void tcpErrorEvent(TcpThread pThread, IOException pException)
    {
        log("ERROR: Connection to quorum server lost, reason: " + pException.getMessage());
        System.exit(0);
    }
}

