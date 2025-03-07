package com.hoddmimes.txtest.server;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hoddmimes.txtest.aux.TxCntx;
import com.hoddmimes.txtest.aux.fe.FEController;
import com.hoddmimes.txtest.aux.ipc.IpcConnectionCallbacks;
import com.hoddmimes.txtest.aux.ipc.IpcEndpoint;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileReader;
import java.net.InetAddress;

public class TxServer implements IpcConnectionCallbacks, FECallbackIf
{
    public enum ServerState { Unknown, Primary, Standby };

    private Logger mLogger = LogManager.getLogger(TxServer.class);
    private JsonObject mConfiguration;
    private String mConfigFilename = "./TxServer.json"; // Default configuration
    private int mNodeId = 0;
    TxExecutor mTxExecutor;
    FEController mFEController;
    AssetController mAssetController;


    public static void main(String[] args) {
        TxServer txServer= new TxServer();
        txServer.parseParameters( args );
        txServer.runServer();
    }



    private void runServer() {
        loadConfiguration();
        createAndStartExecutor();
        creatAndStartAssetController();
        createAndStartFE();
        mLogger.info( "Configuration loaded." );
    }

    private void creatAndStartAssetController() {
        mAssetController = new AssetController( mConfiguration );
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

    public void queueInboundClientMessage(TxCntx pTxCntx) {
        mTxExecutor.queueRequest( pTxCntx );
        pTxCntx.addTimestamp("queued TxCntx");
    }

    public void processClientMessage(TxCntx pTxCntx)
    {
        mAssetController.processClientMessage( pTxCntx );
    }

    @Override
    public void ipcConnectionData(byte[] pMessage, int pServerId, InetAddress pAddress, int pPort) {

    }

    @Override
    public void ipcConnectionStateChange(IpcEndpoint.State pState, int pServerId, InetAddress pIpAddress) {

    }
}
