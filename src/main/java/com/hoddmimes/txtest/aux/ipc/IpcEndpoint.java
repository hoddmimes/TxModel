package com.hoddmimes.txtest.aux.ipc;

import com.google.gson.JsonObject;
import com.hoddmimes.txtest.aux.net.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class IpcEndpoint implements TcpServerCallbackIf, TcpThreadCallbackIf, UdpCallbacksIf
{



    public static enum State {CONNECTED, DISCONNECTED};
    private Logger mLogger;

    private IpcConnectionCallbacks mCallbacks;

    private int mNodeId;
    private String mAddress;
    private int mTcpPort;
    private int mUdpPort;

    private TcpServer mTcpServer;
    private UdpServer mUdpServer;


    public IpcEndpoint(JsonObject jNodeCofiguration, IpcConnectionCallbacks pCallback) {
        mNodeId = jNodeCofiguration.get("node_id").getAsInt();
        mAddress = jNodeCofiguration.get("ip").getAsString();
        mTcpPort = jNodeCofiguration.get("tcp_port").getAsInt();
        mUdpPort = jNodeCofiguration.get("tcp_port").getAsInt();
        mCallbacks = pCallback;
        mLogger = LogManager.getLogger(this.getClass().getSimpleName() + "-node-" + mNodeId );
        declareAndRun();
    }

    public void declareAndRun() {
        try {
            mTcpServer = new TcpServer(this);
            mTcpServer.declareServer(mTcpPort);
            mLogger.info( "declare and started tcp/ip server on port " + mTcpPort);

        } catch (Exception e) {
            mLogger.fatal("failed to declare and start tcp/ip server on port " + mTcpPort);
            System.exit(1);
        }

        try {
            mUdpServer = new UdpServer( mUdpPort, 2048, this );
            mLogger.info( "declare and started UDP server on port " + mTcpPort);
        }
        catch( IOException e ) {
            mLogger.fatal("failed to declare and start UDP server on port " + mUdpPort);
        }


    }


    @Override
    public void tcpInboundConnection(TcpThread pThread) {
        pThread.setCallback(this);

        InetAddress tIpAddress;
        mLogger.info("Inbound connection from " + pThread.getRemoteAddress());
        try { tIpAddress = InetAddress.getByName(pThread.getRemoteAddress());}
        catch( UnknownHostException e) { throw new RuntimeException(e);}
        mCallbacks.ipcConnectionStateChange(IpcEndpoint.State.CONNECTED, mNodeId, tIpAddress);

        pThread.start();
    }

    @Override
    public void tcpMessageRead(TcpThread pThread, byte[] pBuffer) {

    }

    @Override
    public void tcpErrorEvent(TcpThread pThread, IOException pException) {

    }


    @Override
    public void udpError(UdpServer pServer, Exception pException) {

    }

    @Override
    public void udpData(UdpServer pServer, byte[] pData, int pLength, Inet4Address pAddress, int pPort) {

    }
}
