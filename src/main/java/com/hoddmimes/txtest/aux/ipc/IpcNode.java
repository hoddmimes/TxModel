package com.hoddmimes.txtest.aux.ipc;

import com.google.gson.JsonObject;
import com.hoddmimes.msgcompiler.messaging.MessageInterface;
import com.hoddmimes.txtest.aux.net.TcpClient;
import com.hoddmimes.txtest.aux.net.TcpThread;
import com.hoddmimes.txtest.aux.net.TcpThreadCallbackIf;
import com.hoddmimes.txtest.aux.net.UdpClient;
import com.hoddmimes.txtest.generated.ipc.messages.IPCFactory;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/*
    This class handles the communication to remote IpcEndpoints i.e. service (server) endpoints.
    The connection will always try to have an established
 */
public class IpcNode implements TcpThreadCallbackIf {

    private final String                mIpAddress;         // IP address to remote host
    private final int                   mNodeId;            // Global node id in configuration
    private AtomicReference<TcpThread>  mTcpThread;         // Tcp/Ip thread handle to remote host
    private final int                   mUdpPort;           // UDP port where remote host will be listening
    private final int                   mTcpPort;           // Tcp/Ip port to connect to at remote host
    private ConnectThread               mConnectThread;     // Thread working for establishing a connection to remote host in case of being disconnected
    private Logger                      mLogger;            // IPC controller logger
    private IPCFactory                  mIPCFactory;        // IPC message factory
    private IpcCallbacks                mCallbacks;         // parent callback interface

    private volatile long mLastHbTime;



    public IpcNode(JsonObject jNodeConfiguration, Logger pLogger, IpcCallbacks pCallbacks ) {
        mNodeId = jNodeConfiguration.get("node_id").getAsInt();
        mIpAddress = jNodeConfiguration.get("ip").getAsString();
        mUdpPort = jNodeConfiguration.get("udp_port").getAsInt();
        mTcpPort = jNodeConfiguration.get("tcp_port").getAsInt();
        mTcpThread = new AtomicReference<>(null);
        mLogger = pLogger;
        mIPCFactory = new IPCFactory();
        mCallbacks = pCallbacks;
        mConnectThread = new ConnectThread();
        mConnectThread.start();
    }

    boolean isConnected() {
        return (mTcpThread.get() != null);
    }

    void send( MessageInterface pMessage) {
        send( pMessage, true);
    }

    boolean send( MessageInterface pMessage, boolean pUseTcpIp) {
        if (pUseTcpIp) {
            TcpThread tTcpThread = mTcpThread.get();
            if (tTcpThread != null) {
                try {
                    tTcpThread.send(pMessage.getBytes());
                    return true;
                } catch (IOException e) {
                    mLogger.warn("[id: {}] tcp/ip failed to send to node: {}", mNodeId, toString());
                    return false;
                }
            } else {
                return false;
            }
        } else {
                try {
                    UdpClient.send(pMessage.getBytes(), mIpAddress, mUdpPort);
                    return true;
                }
                catch(IOException e) {
                    mLogger.warn("[id: {}] UDP failed to send to node: {}", mNodeId, toString());
                    return false;
                }
            }
    }



    public String toString() {
        boolean tConnected = (mTcpThread.get() != null);

        return String.format("IpcNode [id: %d IP: %s tcpPort: %d udpPort: %d connected: 5s ]", mNodeId, mIpAddress,mTcpPort,mUdpPort, String.valueOf(tConnected));
    }

    public int getNodeId() {
        return mNodeId;
    }

    void hearbeatUpdate() {
        mLastHbTime = System.currentTimeMillis();
        //mLogger.trace("[id: {}] received heartbeat", mNodeId );

    }

    void checkHearbeats( long pHbTimeout) {
        TcpThread tTcpThread = mTcpThread.get();
        if (tTcpThread != null) {
            if ((System.currentTimeMillis() - mLastHbTime) > pHbTimeout) {
                mTcpThread.set(null);
                tTcpThread.close();

                mLogger.warn("[id: {}] [DISCONNECT] (soft) host {}", mNodeId, tTcpThread.toString() );
                mCallbacks.onDisconnect(this, false);
                mConnectThread = new ConnectThread();
                mConnectThread.start();
            }
        }
    }



    @Override
    public void tcpMessageRead(TcpThread pThread, byte[] pBuffer)
    {
        MessageInterface tMsg = mIPCFactory.createMessage(pBuffer);
        if (tMsg == null) {
            mLogger.warn("[id: {}] Failed to create message from buffer, thread: {}", mNodeId, pThread.toString());
            return;
        }

        mLogger.trace("[id: {} (client)] Received message: {}", mNodeId, tMsg.toString());
        mCallbacks.onMessage(new IpcCntx(tMsg, pThread));
    }

    @Override
    public void tcpErrorEvent(TcpThread pThread, IOException pException) {
        TcpThread tTcpThread = mTcpThread.getAndSet(null);
        tTcpThread.close();

        mLogger.warn("[id: {}] [DISCONNECT] (hard) host {}", mNodeId, pThread.toString() );
        mCallbacks.onDisconnect(this, true);
        mConnectThread = new ConnectThread();
        mConnectThread.start();
    }


    class ConnectThread extends Thread
    {

        long    mStartTime;
        int     mConnectionTries;
        long    mLastLogTime;

        long getDismiss() {
            long tDeltaSec = (System.currentTimeMillis() - mStartTime) / 1000L;
            if (tDeltaSec < 30 )
            {
                return 3000L;
            } else if (tDeltaSec < 120) {
                return 5000L;
            } else {
                return 20000L;
            }
        }


        @Override
        public void run() {
            mLastLogTime = 0;
            mStartTime = System.currentTimeMillis();
            mConnectionTries = 0;
            while( true )
            {
                try {
                    TcpThread tTcpThread = TcpClient.connect( IpcNode.this.mIpAddress, IpcNode.this.mTcpPort, IpcNode.this);
                    mTcpThread.set( tTcpThread );
                    mLastHbTime = System.currentTimeMillis();
                    mTcpThread.set( tTcpThread );
                    IpcNode.this.mLogger.info("[id: {}] Successfully connected to {} on port {} retries: {}",
                            mNodeId, IpcNode.this.mIpAddress, IpcNode.this.mTcpPort, mConnectionTries);
                    IpcNode.this.mCallbacks.onConnect(IpcNode.this);
                    return;
                }
                catch( IOException e ) {
                    mConnectionTries++;
                    if ((System.currentTimeMillis() - mLastLogTime) >= 60000L) {
                        mLastLogTime = System.currentTimeMillis();
                        IpcNode.this.mLogger.warn("[id: {}} Failed to connected to {} on port {} retries: {} ",
                                mNodeId, IpcNode.this.mIpAddress, IpcNode.this.mTcpPort, mConnectionTries);
                    }
                }
                try { Thread.sleep(getDismiss());}
                catch (InterruptedException e ) {}
            }
        }
    }
}
