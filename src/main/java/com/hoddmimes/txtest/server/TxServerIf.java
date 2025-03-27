package com.hoddmimes.txtest.server;

import com.google.gson.JsonObject;
import com.hoddmimes.txtest.aux.ipc.IpcController;
import com.hoddmimes.txtest.quorum.QuorumStateController;

public interface TxServerIf
{
    public int getNodeId();
    public String getServiceName();
    public JsonObject getTxlogConfiguration();
    public IpcController getIpcController();
    public QuorumStateController getQSController();
    public JsonObject getConfiguration();
    public ServerMessageSeqnoInterface getMessageSequenceNumberIf();
}
