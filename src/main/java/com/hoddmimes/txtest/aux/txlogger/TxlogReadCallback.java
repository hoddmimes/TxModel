package com.hoddmimes.txtest.aux.txlogger;

public interface TxlogReadCallback
{
    public void txlogReadComplete( byte[] pMessage, long pLogtime, String pFilename );
}
