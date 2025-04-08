import com.hoddmimes.txtest.aux.net.TcpClient;
import com.hoddmimes.txtest.aux.net.TcpThread;
import com.hoddmimes.txtest.aux.net.TcpThreadCallbackIf;
import com.hoddmimes.txtest.generated.ipc.messages.EchoMessage;
import com.hoddmimes.txtest.generated.ipc.messages.IPCFactory;
import org.HdrHistogram.Histogram;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.concurrent.atomic.AtomicInteger;

public class EchoClient
{
    String mHost = "127.0.0.1";
    int mThreads = 1;
    AtomicInteger mTotalSentMsgs = new AtomicInteger(0);
    int mMsgsToSend = 50000;
    boolean mAsyncronous = true;
    Histogram mResponseTimeHistogram;
    IPCFactory mFactory;


    public static void main(String[] args) {
        EchoClient echoClient = new EchoClient();
        echoClient.parseArguments( args );
        echoClient.test();
    }

    private void test() {
        SenderThread[] tThreads = new SenderThread[ mThreads];
        mResponseTimeHistogram = new Histogram(10000000000L, 3);
        mFactory = new IPCFactory();
        for (int i = 0; i < mThreads; i++) {
            tThreads[i] = new SenderThread((i+1));
            tThreads[i].start();
        }
        for (int i = 0; i < mThreads; i++) {
            try {tThreads[i].join();}
            catch( InterruptedException e) {}
        }
        NumberFormat nbf = NumberFormat.getNumberInstance();
        nbf.setMaximumFractionDigits(1);
        System.out.println("Tx count: " + mResponseTimeHistogram.getTotalCount() + " tx-mean: " + nbf.format(mResponseTimeHistogram.getMean()) +
                "  tx-50: " + mResponseTimeHistogram.getValueAtPercentile(50) +
                "  1 stddev (68.27): " + mResponseTimeHistogram.getValueAtPercentile(68.27) +
                "  2 stddev (95.45): " + mResponseTimeHistogram.getValueAtPercentile(95.45) +
                "  3 stddev (99.73): " + mResponseTimeHistogram.getValueAtPercentile(99.73) +
                "  tx-min: " + mResponseTimeHistogram.getMinValue() +
                "  tx-max: " + mResponseTimeHistogram.getMaxValue());
    }

    private void parseArguments( String[] args ) {
        int i = 0;
        while( i < args.length) {
            if (args[i].compareTo("-threads") == 0) {
                mThreads = Integer.parseInt(args[++i]);
            }
            if (args[i].compareTo("-count") == 0) {
                mMsgsToSend = Integer.parseInt(args[++i]);
            }
            if (args[i].compareTo("-async") == 0) {
                mAsyncronous = Boolean.parseBoolean(args[++i]);
            }
            if (args[i].compareTo("-host") == 0) {
                mHost = args[++i];
            }
            i++;
        }
        System.out.println( String.format("[ host: %s Threads: %02d msg-to-send: %d asynchronous: %s ]", mHost, mThreads, mMsgsToSend,String.valueOf(mAsyncronous)));
    }



    class SenderThread extends Thread implements TcpThreadCallbackIf
    {
        int mIndex;
        int mMsgsSent = 0;
        TcpThread mThread;
        Object mThreadEndMutex;
        volatile boolean mAllDone;


        SenderThread( int pIndex ) {
            mIndex = pIndex;
            mThreadEndMutex = new Object();
            mAllDone = false;
        }

        public void run() {
            try {
                if (mAsyncronous) {
                    mThread = TcpClient.connect(mHost, 7878, this);
                } else {
                    mThread = TcpClient.connect(mHost, 7878, null);
                }
                if (!mAsyncronous) {
                    EchoMessage echoMessage = new EchoMessage();

                    while( mTotalSentMsgs.incrementAndGet() < mMsgsToSend) {
                        mMsgsSent++;
                        long tStartTime = System.nanoTime();
                        mThread.transceive(echoMessage.messageToBytes());
                        long tTxTime = System.nanoTime() - tStartTime;
                        synchronized (mResponseTimeHistogram) {
                            mResponseTimeHistogram.recordValue((tTxTime / 1000L));
                        }
                    }
                } else {
                    EchoMessage echoMessage = new EchoMessage();
                    echoMessage.setTime01( System.nanoTime());
                    synchronized ( mThreadEndMutex ) {
                        mThread.send(echoMessage.messageToBytes());
                        if (!mAllDone) {
                            try { mThreadEndMutex.wait(); }
                            catch( InterruptedException e) {}
                        }
                    }
                }
            }
            catch( IOException e) {
                throw new RuntimeException(e);
            }
            System.out.println("Thread " + mIndex + " all done!");
        }

        @Override
        public void tcpMessageRead(TcpThread pThread, byte[] pBuffer) {
            EchoMessage echoMessage = (EchoMessage) mFactory.createMessage(pBuffer);
            long tTxTime = System.nanoTime() - echoMessage.getTime01();
            synchronized (mResponseTimeHistogram) {
                mResponseTimeHistogram.recordValue((tTxTime / 1000L));
            }
            mMsgsSent++;
            if (mTotalSentMsgs.incrementAndGet() < mMsgsToSend ) {
               try {
                   echoMessage.setTime01( System.nanoTime());
                   mThread.send(echoMessage.messageToBytes());
               }
               catch( IOException e) {
                   throw new RuntimeException(e);
               }
            } else {
                synchronized ( mThreadEndMutex ) {
                    mAllDone = true;
                    mThreadEndMutex.notifyAll();
                    System.out.println("Thread [" + mIndex + "] done, sent: " + mMsgsSent + " messages sent.");
                }
            }
        }

        @Override
        public void tcpErrorEvent(TcpThread pThread, IOException pException) {
            System.out.println("Thread Disconnected " + pThread.toString());
        }

    }
}
