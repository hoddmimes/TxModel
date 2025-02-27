package com.hoddmimes.txtest.aux.fe;

import com.hoddmimes.distributor.messaging.MessageInterface;
import com.hoddmimes.txtest.aux.net.TcpThread;

public interface FESendIf
{
    public void sendResponseToClient(TcpThread pThread, MessageInterface pResponse);
}
