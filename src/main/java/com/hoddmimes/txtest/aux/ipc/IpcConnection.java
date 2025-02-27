package com.hoddmimes.txtest.aux.ipc;

import com.google.gson.JsonObject;
import com.hoddmimes.txtest.aux.net.TcpClient;
import com.hoddmimes.txtest.aux.net.TcpThread;
import com.hoddmimes.txtest.aux.net.TcpThreadCallbackIf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/*
    This class handles the communication to remote IpcEndpoints i.e. service (server) endpoints.
    The connection will always try to have an established
 */
public class IpcConnection implements TcpThreadCallbackIf {

    private final Logger mLogger;

    private final String mRmtIpAddress;
    private final int                 mNodeId;
    private volatile TcpThread        mTcpClient;
    private final InetAddress         mRmtUdpAddress;
    private final int                 mRmtUdpPort;
    private final int                 mRmtTcpPort;
    private ConnectThread             mConnectThread;

    public IpcConnection( JsonObject jNodeConfiguration ) {
        mNodeId = jNodeConfiguration.get("node_id").getAsInt();
        mRmtIpAddress = jNodeConfiguration.get("ip").getAsString();
        mRmtUdpPort = jNodeConfiguration.get("udp_port").getAsInt();
        mRmtTcpPort = jNodeConfiguration.get("udp_port").getAsInt();
        mTcpClient = null;
        try {mRmtUdpAddress = InetAddress.getByName(mRmtIpAddress);}
        catch(UnknownHostException e) { throw new RuntimeException(e); }
        mLogger = LogManager.getLogger(this.getClass().getSimpleName() + "-node-" + mNodeId );
        mConnectThread = new ConnectThread();
        mConnectThread.start();
    }


    private void connectAndRun() {
        try {
            mTcpClient = TcpClient.connect(this.mRmtIpAddress, mRmtTcpPort, this);
        }
        catch( IOException e) {

        }
    }

    @Override
    public void tcpMessageRead(TcpThread pThread, byte[] pBuffer) {

    }

    @Override
    public void tcpErrorEvent(TcpThread pThread, IOException pException) {
        synchronized( this ) {
            mTcpClient = null;
        }
        mLogger.warn("tcp/ip connection lost to " + pThread.toString() );

    }


    class ConnectThread extends Thread
    {

        long mStartTime;
        int mConnectionTries;
        long mLastLogTime;

        long getDismiss() {
            long tDeltaSec = (System.currentTimeMillis() - mStartTime) / 1000L;
            if (tDeltaSec < 30 )
            {
                return 3000L;
            } else if (tDeltaSec < 120) {
                return 10000L;
            } else {
                return 30000L;
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
                    IpcConnection.this.mTcpClient = TcpClient.connect( IpcConnection.this.mRmtIpAddress, IpcConnection.this.mRmtTcpPort, IpcConnection.this);
                    IpcConnection.this.mLogger.info("successfully connected to " + IpcConnection.this.mRmtIpAddress + " on port " + IpcConnection.this.mRmtTcpPort + " retries: " + mConnectionTries);
                    return;
                }
                catch( IOException e ) {
                    mConnectionTries++;
                    if ((System.currentTimeMillis() - mLastLogTime) >= 60000L) {
                        mLastLogTime = System.currentTimeMillis();
                        IpcConnection.this.mLogger.warn("failed to connected to " + IpcConnection.this.mRmtIpAddress + " on port " + IpcConnection.this.mRmtTcpPort + " retries: " + mConnectionTries);
                    }
                }
            }
        }
    }
}
