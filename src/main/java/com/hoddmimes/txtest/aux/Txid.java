package com.hoddmimes.txtest.aux;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;


public class Txid {
    static long mNetwrk = (findNetworkAddress() << 48);
    static long mTime = ((System.currentTimeMillis() / 1000L) & 0xffff) << 32;
    static AtomicLong mCounter = new AtomicLong(0);




    public static void main(String[] args) {
        for (int i = 0; i < 100; i++) {
            System.out.println( " txid: " + Long.toHexString(Txid.next()));
        }
    }




    public static  long next() {
        if (mCounter.get() >= 0xfffffff) {
            synchronized (Txid.class) {
                if (mCounter.get() >= 0xfffffff) {
                    mTime = ((System.currentTimeMillis() / 1000L) & 0xffff) << 32;
                    mCounter.set(0);
                }
            }
        }
        long x = mCounter.incrementAndGet();
        return (mNetwrk + mTime + x);
    }




    private static long findNetworkAddress() {
        try {
            Iterator<NetworkInterface> tNetItr = NetworkInterface.getNetworkInterfaces().asIterator();
            while (tNetItr.hasNext()) {
                NetworkInterface ni = tNetItr.next();
                Iterator<InetAddress> tAdrItr = ni.getInetAddresses().asIterator();
                while (tAdrItr.hasNext()) {
                    InetAddress tAddr = tAdrItr.next();
                    if (!tAddr.isLoopbackAddress()) {
                        if (tAddr instanceof Inet4Address) {
                            System.out.println(" device: " + ni.getDisplayName());
                            return (long) bytesToInt(tAddr.getAddress());
                        }
                    }
                }
            }
            Random r = new Random(System.nanoTime());
            return Math.abs( r.nextInt() );
        } catch (IOException e) {
            e.printStackTrace();
        }
        Random r = new Random(System.nanoTime());
        return Math.abs( r.nextLong() );
    }

    private static int bytesToInt(byte[] pBuffer )
    {
        int tValue = 0;
        tValue += ((pBuffer[0] & 0xff) << 24);
        tValue += ((pBuffer[1] & 0xff) << 16);
        tValue += ((pBuffer[2] & 0xff) << 8);
        tValue += ((pBuffer[3] & 0xff) << 0);
        return tValue;
    }
}
