package com.hoddmimes.txtest.aux;


import com.hoddmimes.distributor.messaging.MessageInterface;
import com.hoddmimes.txtest.aux.fe.FESendIf;
import com.hoddmimes.txtest.aux.net.TcpThread;
import com.hoddmimes.txtest.generated.fe.messages.RequestMessage;

public class TxCntx
{
    public RequestMessage mRequest;
    public AuxTimestamp         mTimestamps;

    private FESendIf             mSendInterface;
    private TcpThread            mTcpThread;

    public TxCntx( FESendIf pSendInterface, TcpThread pThread, MessageInterface pRequest) {
        mTcpThread = pThread;
        mRequest = (RequestMessage) pRequest;
        mSendInterface = pSendInterface;
        mTimestamps = new AuxTimestamp("created TxCntx");
    }

    public TxCntx( FESendIf pSendInterface, TcpThread pThread) {
        mTcpThread = pThread;
        mRequest = null;
        mSendInterface = pSendInterface;
        mTimestamps = new AuxTimestamp("created TxCntx");
    }

    public void setRequest(RequestMessage mRequest) {
        this.mRequest = mRequest;
    }

    public void sendResponse(MessageInterface pResponse) {
        mSendInterface.sendResponseToClient( mTcpThread, pResponse );
    }

    public void addTimestamp( String pMark)
    {
        mTimestamps.add( pMark );
    }



}
