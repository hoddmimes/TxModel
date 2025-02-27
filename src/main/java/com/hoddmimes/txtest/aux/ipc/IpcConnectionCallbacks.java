package com.hoddmimes.txtest.aux.ipc;

import java.net.InetAddress;

public interface IpcConnectionCallbacks
{
    public void ipcConnectionData(byte[] pMessage, int pServerId, InetAddress pAddress, int pPort );
    public void ipcConnectionStateChange(IpcEndpoint.State pState, int pServerId, InetAddress pIpAddress);
}


