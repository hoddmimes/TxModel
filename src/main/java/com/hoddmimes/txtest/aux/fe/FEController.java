package com.hoddmimes.txtest.aux.fe;

import com.hoddmimes.distributor.messaging.MessageInterface;
import com.hoddmimes.txtest.aux.TxCntx;
import com.hoddmimes.txtest.aux.net.TcpServer;
import com.hoddmimes.txtest.aux.net.TcpServerCallbackIf;
import com.hoddmimes.txtest.aux.net.TcpThread;
import com.hoddmimes.txtest.aux.net.TcpThreadCallbackIf;
import com.hoddmimes.txtest.generated.fe.messages.FEFactory;
import com.hoddmimes.txtest.generated.fe.messages.RequestMessage;
import com.hoddmimes.txtest.server.FECallbackIf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class FEController implements FESendIf, TcpServerCallbackIf, TcpThreadCallbackIf {

    String      mIpInterface;
    int         mPort;
    int         mNodeId;
    TcpServer   mTcpServer;
    Logger      mLogger;
    FECallbackIf mCallbackIf;
    FEFactory   mMsgFactory;

    public FEController(int pNodeId, String pIpInterface, int pPort, FECallbackIf pCallback) {
        mLogger = LogManager.getLogger( this.getClass().getSimpleName() + "-" + pNodeId);
        mIpInterface = pIpInterface;
        mNodeId = pNodeId;
        mPort = pPort;
        mCallbackIf = pCallback;
        mMsgFactory = new FEFactory();

        declareAndRun();
    }

    private void declareAndRun() {
        try {
            mTcpServer = new TcpServer( this);
            mTcpServer.declareServer( mIpInterface, mPort);
            mLogger.info("declared and starting FE interface on port " + mPort);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sendResponseToClient(TcpThread pThread, MessageInterface pResponse) {
        try {
            pThread.send( pResponse.messageToBytes());
        }
        catch(Exception e) {
            mLogger.warn("failed to send response to " + pThread.getRemoteAddress(), e);
            pThread.close();
        }
    }


    @Override
    public void tcpInboundConnection(TcpThread pThread) {
        mLogger.info(" FE inbound connection from " + pThread.getRemoteAddress());
        pThread.setCallback( this );
        pThread.start();
    }

    @Override
    public void tcpMessageRead(TcpThread pThread, byte[] pBuffer)
    {

        TxCntx txCntx = new TxCntx( this, pThread);
        MessageInterface mRqstMsg = mMsgFactory.createMessage( pBuffer );
        txCntx.setRequest( (RequestMessage) mRqstMsg );
        txCntx.addTimestamp("added request message");
        mCallbackIf.queueInboundClientMessage( txCntx );
    }

    @Override
    public void tcpErrorEvent(TcpThread pThread, IOException pException) {
        mLogger.warn(" FE disconnected, client " + pThread.getRemoteAddress(), pException );
        pThread.close();
    }
}
