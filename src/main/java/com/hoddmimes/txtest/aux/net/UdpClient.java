package com.hoddmimes.txtest.aux.net;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;

public class UdpClient
{
    private Inet4Address mAddress;
    private int mPort;

    public UdpClient( Inet4Address pAddress, int pPort ) {
        mAddress = pAddress;
        mPort = pPort;
    }

    public void send( byte[] pData ) throws IOException
    {
        send(pData, mAddress, mPort);
    }


    public static void send(byte[] pData, String pServerAddress, int pServerPort ) throws IOException {
        send(pData, InetAddress.getByName(pServerAddress), pServerPort);
    }

    public static void send(byte[] pData, InetAddress pServerAddress, int pServerPort ) throws IOException {
        DatagramSocket tSocket = new DatagramSocket();
        DatagramPacket sendPacket = new DatagramPacket(pData, pData.length, pServerAddress, pServerPort);
        tSocket.send(sendPacket);
    }
}
