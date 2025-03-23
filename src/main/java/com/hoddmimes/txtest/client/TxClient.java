package com.hoddmimes.txtest.client;


import com.hoddmimes.txtest.aux.net.TcpClient;
import com.hoddmimes.txtest.aux.net.TcpThread;
import com.hoddmimes.txtest.generated.fe.messages.FEFactory;
import com.hoddmimes.txtest.generated.fe.messages.UpdateMessage;
import com.hoddmimes.txtest.generated.fe.messages.UpdateResponse;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TxClient
{
    private int mAssetCount = 100;
    private String mHost = "127.0.0.1";
    private int mPort = 4001;

    private AtomicInteger mTxMsgsSent = new AtomicInteger(0);
    private int mTxToSend = 25000;
    private int mSenderThreads = 1;
    private double mTxRate = 0.0;
    private RateCalculator mRateCalculator;
    private AtomicLong mTotTxTime = new AtomicLong(0);
    private AtomicLong mTotTxCount = new AtomicLong(0);

    public static void main(String[] args) {
        TxClient clt = new TxClient();
        clt.parseArguments( args );
        clt.test();

    }


    private void test() {

        mRateCalculator = new RateCalculator(mSenderThreads, mTxRate);

        FEFactory tMsgFactory = new FEFactory();
        SenderThread[] tThreads = new SenderThread[ mSenderThreads ];
        long tTxStartTime;
        long tTxAVgTime = 0;
        long tTxCount = 0;

        tTxStartTime = System.nanoTime();
        for (int i = 0; i < mSenderThreads; i++) {
            tThreads[i] = new SenderThread((i+1));
            tThreads[i].start();
        }
        for (int i = 0; i < mSenderThreads; i++) {
            try {
                tThreads[i].join();
            }
            catch (InterruptedException e) {}
        }
        tTxAVgTime = (mTotTxTime.get() / 1000L) / mTotTxCount.get();
        System.out.println("All done, tx_count: " + mTotTxCount.get() + " avg tx end-to-end time: " + tTxAVgTime + " usec.");
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
            if( args[i].equals( "-count" ) ) {
                mTxToSend = Integer.parseInt(args[++i]);
            }
            if( args[i].equals( "-threads" ) ) {
                mSenderThreads = Integer.parseInt(args[++i]);
            }
            if( args[i].equals( "-rate" ) ) {
                mTxRate = Double.parseDouble(args[++i]);
            }
            i++;
        }
    }




    class RateCalculator {
        long mDismiss;
        int  mBatchFactor;


        private void calculate(int pThreads, double pTxRate) {
            if (pTxRate == 0.0) {
                mDismiss = 0;
                mBatchFactor = 1;
            } else {
                if (pTxRate <= 500) {
                    mDismiss = (long) ((1000.0 * (double) pThreads) / (double) mTxRate);
                    mBatchFactor = 1;
                } else if (pTxRate < 2000) {
                    mBatchFactor = 8;
                    mDismiss = mDismiss = (long) ((1000.0 * (double) pThreads * mBatchFactor)  / (double) mTxRate);
                } else if (pTxRate < 2000) {
                    mBatchFactor = 16;
                    mDismiss = mDismiss = (long) ((1000.0 * (double) pThreads * mBatchFactor)  / (double) mTxRate);
                } else {
                    mBatchFactor = 50;
                    mDismiss = mDismiss = (long) ((1000.0 * (double) pThreads * mBatchFactor)  / (double) mTxRate);
                }
            }
        }

        long getDismiss() {
            return mDismiss;
        }

        int getBatchFactor() {
            return mBatchFactor;
        }

        RateCalculator( int pThreads, double pTxRate ) {
            calculate(pThreads, pTxRate);
        }
    }


    class SenderThread extends Thread {
        int mThreadIndex;
        Random mRnd;
        int mThreadMsgsSent;
        private TcpThread mClient;
        private FEFactory mFactory;

        SenderThread( int pThreadIndex ) {
            mThreadIndex = pThreadIndex;
            mThreadMsgsSent = 0;
            mFactory = new FEFactory();
            mRnd = new Random(System.nanoTime() + 937 * mThreadIndex);
        }

        private void createConnection() {
            try {
                mClient = TcpClient.connect( mHost, mPort, null);
            }
            catch( Exception e) {
                throw new RuntimeException(e);
            }
        }

        private UpdateMessage createUpdateMessage(int pCount) {
            UpdateMessage upd = new UpdateMessage();
            upd.setAssetId( 1 + mRnd.nextInt(mAssetCount));
            int tValue = mRnd.nextInt(100) * (mRnd.nextBoolean() ? -1 : 1);
            upd.setValue(tValue);
            upd.setRequestId( pCount );
            return upd;
        }


        @Override
        public void run() {
            long tTxStartTime;
            long dismissTime = mRateCalculator.getDismiss();
            int mBatchFactor = mRateCalculator.getBatchFactor();
            System.out.println("Starting Thread " + mThreadIndex + " dismiss time: " + dismissTime + " batch factor: " + mBatchFactor);
            int x;
            createConnection();
            while (mTxMsgsSent.get() <= mTxToSend) {
                x = 0;
                while(((x++) < mBatchFactor) && (mTxMsgsSent.incrementAndGet() <= mTxToSend)) {
                    mThreadMsgsSent++;
                    UpdateMessage upd = createUpdateMessage(mTxMsgsSent.get());
                    try {
                        tTxStartTime = System.nanoTime();
                        byte[] tRspBuffer = mClient.transceive(upd.messageToBytes());
                        UpdateResponse tResponse = (UpdateResponse) mFactory.createMessage(tRspBuffer);
                        mTotTxTime.addAndGet((System.nanoTime() - tTxStartTime));
                        mTotTxCount.incrementAndGet();
                        if (!tResponse.getStatusOk()) {
                            System.out.println("ERROR msg: " + mThreadMsgsSent + " asset: " + upd.getAssetId() + " response: " + tResponse.getStatusText());
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                try {
                    Thread.sleep(dismissTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            System.out.println("Sender thread " + mThreadIndex + " done (" + mThreadMsgsSent +")");
        }


    }
}
