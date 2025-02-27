package com.hoddmimes.txtest.aux.net;

import java.net.Inet4Address;
import java.net.DatagramSocket;
import java.net.DatagramPacket;
import java.io.IOException;

public class UdpServer extends Thread
{
    private int mBufferSize;
    private int mPort;
    private DatagramSocket mServerSocket;
    private volatile boolean mClosed;
    private UdpCallbacksIf mCallbacks;

    public UdpServer(int port, int pBufferSize, UdpCallbacksIf pCallbacks) throws IOException
    {
        mCallbacks = pCallbacks;
        mBufferSize = pBufferSize;
        mClosed = false;
        mPort = port;
        DatagramSocket mServerSocket = new DatagramSocket(mPort);
        this.start();
    }

    public int getPort()
    {
        return mPort;
    }

    public void close() {
        if (!mClosed) {
            mClosed = true;
            mServerSocket.close();
        }
    }

    @Override
    public void run() {
        while (!mClosed) {
            try {
                byte[] buffer = new byte[mBufferSize];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                mServerSocket.receive(packet);
                mCallbacks.udpData(this, packet.getData(), packet.getLength(), (Inet4Address) packet.getAddress(), packet.getPort() );
            } catch (IOException e) {
                if (!mClosed) {
                    mCallbacks.udpError(this, e);
                    return;
                }
            }
        }
    }

}
