package com.hoddmimes.txtest.aux.ipc;

import com.google.gson.JsonObject;
import com.hoddmimes.msgcompiler.messaging.MessageInterface;
import com.hoddmimes.txtest.aux.net.*;
import com.hoddmimes.txtest.generated.ipc.messages.Heartbeat;
import com.hoddmimes.txtest.generated.ipc.messages.IPCFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Inet4Address;

public class IpcEndpoint implements TcpServerCallbackIf, TcpThreadCallbackIf, UdpCallbacksIf
{



    public static enum State {CONNECTED, DISCONNECTED};
    private Logger mLogger;

    private IpcCallbacks mCallbacks;

    private int mNodeId;
    private String  mAddress;
    private int     mTcpPort;
    private int     mUdpPort;

    private TcpServer mTcpServer;
    private UdpServer mUdpServer;
    private IPCFactory mIpcFactory = new IPCFactory();


    public IpcEndpoint(JsonObject jNodeCofiguration, IpcCallbacks pCallback) {
        mNodeId = jNodeCofiguration.get("node_id").getAsInt();
        mAddress = jNodeCofiguration.get("ip").getAsString();
        mTcpPort = jNodeCofiguration.get("tcp_port").getAsInt();
        mUdpPort = jNodeCofiguration.get("udp_port").getAsInt();
        mCallbacks = pCallback;
        mLogger = LogManager.getLogger( IpcEndpoint.class );
        declareAndRun();
    }

    public void declareAndRun() {
        try {
            mTcpServer = new TcpServer(this);
            mTcpServer.declareServer(mTcpPort);
            mLogger.info( "Declared IPC TCP/IP Endpoint started tcp/ip server on port " + mTcpPort);

        } catch (Exception e) {
            mLogger.fatal("failed to declare and start tcp/ip server on port " + mTcpPort + " reason: " + e.getMessage());
            throw new RuntimeException(e);
        }

        try {
            mUdpServer = new UdpServer( mUdpPort, 2048, this );
            mLogger.info( "Declared IPC UDP Endpoint and started on port " + mUdpPort);
        }
        catch( IOException e ) {
            mLogger.fatal("failed to declare and start UDP server on port " + mUdpPort);
            throw new RuntimeException(e);
        }


    }


    @Override
    public void tcpInboundConnection(TcpThread pThread) {
        pThread.setCallback(this);
        mLogger.info("[INBOUND] client " + pThread.getRemoteAddress());
        pThread.start();
    }

    @Override
    public void tcpMessageRead(TcpThread pThread, byte[] pBuffer) {
        MessageInterface tMessage = mIpcFactory.createMessage(pBuffer);
        if (tMessage == null) {
            mLogger.warn("Unknown tcp/ip message from" + pThread.getRemoteAddress());
            return;
        }

        if (tMessage.getMessageId() != Heartbeat.MESSAGE_ID) {
            mLogger.trace("[id: {} (endpoint)] Received message: {}", mNodeId, tMessage.toString());
        }
        mCallbacks.onMessage( new IpcCntx( tMessage, pThread));
    }

    @Override
    public void tcpErrorEvent(TcpThread pThread, IOException pException) {
        mLogger.warn("[DISCONNECT] client " + pThread.getRemoteAddress());
    }


    @Override
    public void udpError(UdpServer pServer, Exception pException) {
        mLogger.error("UDP server error, reason: " + pException.getMessage());
    }

    @Override
    public void udpData(UdpServer pServer, byte[] pData, int pLength, Inet4Address pAddress, int pPort) {
        byte[] tBuffer = new byte[pLength];
        System.arraycopy(pData, 0, tBuffer, 0, pLength);

        MessageInterface tMessage = mIpcFactory.createMessage(tBuffer);
        if (tMessage == null) {
            mLogger.warn("Unknown UDP message from" + pAddress.getHostAddress() + ":" + pPort);
            return;
        }
        mCallbacks.onMessage( new IpcCntx( tMessage, pAddress, pPort));
    }
}
