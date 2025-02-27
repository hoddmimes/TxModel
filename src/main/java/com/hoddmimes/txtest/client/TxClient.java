package com.hoddmimes.txtest.client;

import com.hoddmimes.distributor.messaging.MessageInterface;
import com.hoddmimes.txtest.aux.net.TcpClient;
import com.hoddmimes.txtest.aux.net.TcpThread;
import com.hoddmimes.txtest.generated.fe.messages.FEFactory;
import com.hoddmimes.txtest.generated.fe.messages.UpdateMessage;

import java.io.IOException;
import java.util.Random;

public class TxClient
{
    private String mHost = "127.0.0.1";
    private int mPort = 4001;
    private TcpThread mClient;
    private Random mRnd;


    public static void main(String[] args) {
        TxClient clt = new TxClient();
        clt.parseArguments( args );
        clt.createConnection();
        clt.test();

    }

    public TxClient() {
        mRnd = new Random();
    }

    private void test() {
        FEFactory tMsgFactory = new FEFactory();

        for (int i = 0; i < 100; i++) {
            UpdateMessage upd = createUpdateMessage((i+1));
            try {
                byte[] tbuffer = mClient.transceive( upd.messageToBytes());
                MessageInterface tMsg = tMsgFactory.createMessage( tbuffer );
                if (tMsg != null) {
                    System.out.println(tMsg.toString());
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private UpdateMessage createUpdateMessage( int pCount) {
        UpdateMessage upd = new UpdateMessage();
        upd.setAssetId( 1 + mRnd.nextInt(20));
        upd.setValue( Math.abs(mRnd.nextInt()));
        upd.setRequestId( pCount );
        upd.setInitTime( System.currentTimeMillis());
        return upd;
    }


    private void createConnection() {
        try {
            mClient = TcpClient.connect( mHost, mPort, null);
        }
        catch( Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void parseArguments( String[] args ) {
        int i = 0;
        while( i < args.length ) {
            if( args[i].equals( "-host" ) ) {
                mHost = args[++i];
            }
            if( args[i].equals( "-port" ) ) {
                mPort = Integer.parseInt(args[++i]);
            }
        }
    }
}
