package com.hoddmimes.txtest.aux;


import com.hoddmimes.msgcompiler.messaging.MessageInterface;
import com.hoddmimes.txtest.aux.fe.FESendIf;
import com.hoddmimes.txtest.aux.net.TcpThread;
import com.hoddmimes.txtest.generated.fe.messages.RequestMessage;

public class TxCntx
{
    private RequestMessage mRequest;   // The FE request message
    public AuxTimestamp           mTimestamps;    // A class for collecting and printing timestamps of the transaction execution
    private long                  mMessageSeqno; // Global service / server
    private boolean               mReplicated; // Flag set to true when the standby have confirmed the replication
    private boolean               mPrimaryTx; // True when the tx received from a FE client and when being in primary mode
    private boolean               mReplay; // Transaction is executed as part of an intial replay, no client or standby communitcation is done
    private int                  mAssetId;
    private FESendIf             mSendInterface;
    private TcpThread            mTcpThread;
    private long                    mTxid;

    // Constructor when being primary and transaction is created by a message from a FE client
    public TxCntx( FESendIf pSendInterface, TcpThread pThread, MessageInterface pRequest) {
        mTcpThread = pThread;
        mRequest = (RequestMessage) pRequest;
        mSendInterface = pSendInterface;
        mMessageSeqno = 0L;
        mTxid = 0L;
        mReplicated = false;
        mPrimaryTx = true;
        mReplay = false;
        mTimestamps = new AuxTimestamp("created TxCntx");
    }

    public long getMessageSequenceNumber() {
        return mMessageSeqno;
    }
    public int getAssetId() {
        return mRequest.getAssetId();
    }

    public boolean isReplicated() {
        return mReplicated;
    }

    public boolean isPrimaryTx() {
        return mPrimaryTx;
    }

    public boolean isReplay() {
        return mReplay;
    }

    public RequestMessage getRequestMessage() {
        return mRequest;
    }

    // Constructor when being primary and transaction is created by a message from a FE client
    public TxCntx( FESendIf pSendInterface, TcpThread pThread) {
        this( pSendInterface, pThread, null);
    }

    // Constructor when the transaction is executing in the context of a standby
    public TxCntx( MessageInterface pMessage, long pSeqno, long pTxid) {
        mTcpThread = null;
        mRequest = (RequestMessage) pMessage;
        mSendInterface = null;
        mMessageSeqno = pSeqno;
        mTxid = pTxid;
        mReplicated = false;
        mReplay = false;
        mPrimaryTx = false;
        mTimestamps = new AuxTimestamp("created stdby TxCntx");
    }

    // Constructor when being created as part of a reply
    public TxCntx( MessageInterface pMessage ) {
        mTcpThread = null;
        mRequest = (RequestMessage) pMessage;
        mSendInterface = null;
        mMessageSeqno = 0L;
        mReplicated = false;
        mReplay = true;
        mPrimaryTx = false;
        mTimestamps = new AuxTimestamp("created stdby TxCntx");
    }


    public void setMessageSequenceNumber( long pSeqno ) {
        mMessageSeqno = pSeqno;
    }

    public void setRequest(RequestMessage mRequest) {
        this.mRequest = mRequest;
    }

    public void setTxid( long pTxid ) {
        this.mTxid = pTxid;
    }

    public long getTxid() {
        return mTxid;
    }

    public void sendResponse(MessageInterface pResponse) {
        mSendInterface.sendResponseToClient( mTcpThread, pResponse );
    }


    public void addTimestamp( String pMark )
    {
        mTimestamps.add( pMark );
    }

    public void setReplicatedToStandby() {
        mTimestamps.add("About to set replicated to standby");
        synchronized ( this ) {
            this.mReplicated = true;
            this.notifyAll();
        }
    }
}
