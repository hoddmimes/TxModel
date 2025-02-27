package com.hoddmimes.txtest.aux.net;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class UdpClient
{
    public static void send(byte[] pData, String pServerAddress, int pServerPort ) throws IOException {
        send(pData, InetAddress.getByName(pServerAddress), pServerPort);
    }

    public static void send(byte[] pData, InetAddress pServerAddress, int pServerPort ) throws IOException {
        DatagramSocket tSocket = new DatagramSocket();
        DatagramPacket sendPacket = new DatagramPacket(pData, pData.length, pServerAddress, pServerPort);
        tSocket.send(sendPacket);
    }
}
