package com.hoddmimes.txtest.aux.ipc;

public interface IpcCallbacks
{
    public void onMessage(IpcCntx pIpcCntx);
    public void onConnect( IpcNode pIpcNode );
    public void onDisconnect( IpcNode pIpcNode, boolean pHardDisconnect );
}


