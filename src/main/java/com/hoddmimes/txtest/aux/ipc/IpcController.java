package com.hoddmimes.txtest.aux.ipc;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.hoddmimes.msgcompiler.messaging.MessageInterface;
import com.hoddmimes.txtest.generated.ipc.messages.Heartbeat;
import com.hoddmimes.txtest.generated.ipc.messages.IPCFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class IpcController extends Thread implements IpcCallbacks
{
    private final JsonObject    jConfiguration;
    private final int           mNodeId;
    private List<IpcCallbacks>  mCallbacksListiners;

    private Logger mLogger = LogManager.getLogger( IpcController.class );
    private List<IpcNode>       mConnections;
    private IpcEndpoint         mEndPoint;
    private IPCFactory          mIPCFactory;


    public IpcController(int pNodeId, JsonObject pConfiguration ) {
        mConnections = new java.util.ArrayList<>();
        mCallbacksListiners = new ArrayList<>();
        mIPCFactory = new IPCFactory();
        jConfiguration = pConfiguration;
        mNodeId = pNodeId;
        declareAndRun();
    }



    public boolean isNodeConnected( int pNodeId ) {
        IpcNode tNode = getIpcNode(pNodeId );
        return tNode.isConnected();
    }

    public void addIpcCallback(IpcCallbacks pCallback ) {
        synchronized (mCallbacksListiners) {
            mCallbacksListiners.add(pCallback);
        }
    }

    private IpcNode getIpcNode( int pNodeId ) {
        for( IpcNode t : mConnections ) {
            if (t.getNodeId() == pNodeId) {
               return t;
            }
        }
        throw new RuntimeException(String.format("Invalid reference to IPC node id: {}", pNodeId));
    }

    public boolean send(int pNodeId, MessageInterface pMessage) {
        return send(pNodeId, pMessage, true);
    }


    public boolean  send(int pNodeId, MessageInterface pMessage, boolean pUseTcpIp) {
        IpcNode tNode = getIpcNode(pNodeId);
        return tNode.send( pMessage, pUseTcpIp);
    }

    private void declareAndRun() {
     try {
         JsonObject jNode = getJsonNode(mNodeId); // Get the node configuration for this node
         mEndPoint = new IpcEndpoint( jNode, this );
         mLogger.info("Starting server on interface " + jNode.get("ip").getAsString() + " port " + jNode.get("tcp_port").getAsInt() );

         // Create a connection to all nodes, this node included
         JsonArray jNodes = jConfiguration.get("nodes").getAsJsonArray();
         for (int i = 0; i < jNodes.size(); i++) {
             IpcNode tNode = new IpcNode(jNodes.get(i).getAsJsonObject(), mLogger, this);
             mConnections.add(tNode);
         }
         this.start();
     } catch (Exception e) {
         throw new RuntimeException(e);
     }
    }

    private JsonObject getJsonNode( int pNodeId) {
        JsonArray jNodes = jConfiguration.get("nodes").getAsJsonArray();
        for (int i = 0; i < jNodes.size(); i++) {
            if (pNodeId == jNodes.get(i).getAsJsonObject().get("node_id").getAsInt()) {
                return jNodes.get(i).getAsJsonObject();
            }
        }
        return null;
    }

    private void processHearbeat( Heartbeat pHbMessage ) {
        IpcNode tNode = getIpcNode(pHbMessage.getNodeId());
        tNode.hearbeatUpdate();
    }


    public void run() {
        long tHbInterval = jConfiguration.get("ipc_connections").getAsJsonObject().get("hb_intervals_sec").getAsLong() * 1000L;
        long tMaxMissedHb = jConfiguration.get("ipc_connections").getAsJsonObject().get("max_missed_hb").getAsLong();
        while( true ) {
            try {Thread.sleep(tHbInterval);}
            catch(InterruptedException e) {}

            Heartbeat hb = new Heartbeat();
            hb.setNodeId( mNodeId );

            for( IpcNode tNode : mConnections ) {
                tNode.send( hb );
                tNode.checkHearbeats((tHbInterval * tMaxMissedHb));
            }
        }
    }

    public void addCallbackListener( IpcCallbacks pCallback ) {
        synchronized (mCallbacksListiners) {
            mCallbacksListiners.add(pCallback);
        }
    }

    @Override
    public void onMessage(IpcCntx pIpcCntx) {
        if (pIpcCntx.getMessage().getMessageId() == Heartbeat.MESSAGE_ID) {
            processHearbeat((Heartbeat) pIpcCntx.getMessage());
            return;
        }

        synchronized (mCallbacksListiners) {
            for (IpcCallbacks tCallback : mCallbacksListiners) {
                tCallback.onMessage(pIpcCntx);
            }
        }
    }

    @Override
    public void onConnect(IpcNode pIpcNode) {
        synchronized (mCallbacksListiners) {
            for (IpcCallbacks tCallback : mCallbacksListiners) {
                tCallback.onConnect(pIpcNode);
            }
        }
    }

    @Override
    public void onDisconnect(IpcNode pIpcNode, boolean pHardDisconnect) {
        synchronized (mCallbacksListiners) {
            for (IpcCallbacks tCallback : mCallbacksListiners) {
                tCallback.onDisconnect(pIpcNode, pHardDisconnect);
            }
        }
    }
}
