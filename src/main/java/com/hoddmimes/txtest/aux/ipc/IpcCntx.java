package com.hoddmimes.txtest.aux.ipc;


import com.hoddmimes.msgcompiler.messaging.MessageInterface;
import com.hoddmimes.txtest.aux.net.TcpThread;
import com.hoddmimes.txtest.aux.net.UdpClient;

import java.io.IOException;
import java.net.Inet4Address;

public class IpcCntx
{
    private MessageInterface mMessage;
    private TcpThread mTcpThread;
    private Inet4Address mUdpAddress;
    private int mUdpPort;


    public IpcCntx( MessageInterface pMessage, Inet4Address pUdpAddress, int pUdpPort) {
        mMessage = pMessage;
        mTcpThread = null;
        mUdpAddress = pUdpAddress;
        mUdpPort = pUdpPort;
    }

    public MessageInterface getMessage() {
        return mMessage;
    }



    public IpcCntx( MessageInterface pMessage, TcpThread pTcpThread) {
        mMessage = pMessage;
        mTcpThread = pTcpThread;
        mUdpAddress = null;
        mUdpPort = 0;
    }

    public void send(MessageInterface pMessage) throws IOException {
        if (mTcpThread != null) {
            mTcpThread.send( pMessage.getBytes() );
        } else {
            UdpClient.send( pMessage.getBytes(), mUdpAddress, mUdpPort );
        }
    }
}
