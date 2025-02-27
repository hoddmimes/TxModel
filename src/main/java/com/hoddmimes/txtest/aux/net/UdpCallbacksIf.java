package com.hoddmimes.txtest.aux.net;

import java.net.Inet4Address;

public interface UdpCallbacksIf
{
    public void udpError( UdpServer pServer,  Exception pException );
    public void udpData(UdpServer pServer, byte[] pData, int pLength, Inet4Address pAddress, int pPort );
}
