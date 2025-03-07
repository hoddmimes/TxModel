
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;



public class LMAX
{
    private static int cLOOPS = 100000;
    private static int cPRODUCERS = 6;

    long     mTotalSum;
    long     mEventCount;

    volatile boolean  mAllDone;
    Object  mAllDoneMutex;

    Disruptor<Event> mDisruptor;


    public static void main(String[] args) {
        LMAX t = new LMAX();
        t.test();
    }

    private void test() {
        Producer producers[] = new Producer[cPRODUCERS];

        mEventCount = 0;
        mTotalSum = 0;
        mAllDone = false;
        mAllDoneMutex = new Object();

        int tBufferSize = 32768;
        ExecutorService tExecutor = Executors.newCachedThreadPool();
        MyEventFactory tEventFactory = new MyEventFactory();
        MyEventHandler tEventHandler = new MyEventHandler();

        mDisruptor = new Disruptor<>(
                tEventFactory,
                tBufferSize,
                tExecutor,
                ProducerType.MULTI,
                new BlockingWaitStrategy() // BlockingWaitStrategy simulates a blocking queue like LinkedBlockingQueue
        );
        mDisruptor.handleEventsWith( tEventHandler );
        mDisruptor.start();


        for (int i = 0; i < producers.length; i++) {
            Producer p = new Producer((i+1));
            p.start();
            producers[i] = p;
        }

        try {Thread.sleep(1000);}
        catch( InterruptedException e) {};


        long tStartTime = System.nanoTime();
        for (int i = 0; i < producers.length; i++) {
            synchronized( producers[i].mutex) {
                producers[i].mutex.notifyAll();
            }
        }

        synchronized ( mAllDoneMutex ) {
            if (!mAllDone) {
                try {
                    System.out.println("Work to do, waiting for consumer");
                    mAllDoneMutex.wait();
                } catch (InterruptedException e) {
                }
            }
        }

        long tExecTime = (System.nanoTime() - tStartTime) / 1000L;
        System.out.println("TotalSum: " + mTotalSum + " exec time: " + tExecTime + " ms.");

    }



    class Producer extends Thread
    {
        Object mutex = new Object();  // To synchronize producer and consumer threads.
        final int mIndex;
        int i;

        Producer( int pIndex ) {
            mIndex = pIndex;
        }
        public void run() {
            System.out.println("Starting producer " + mIndex );
            synchronized ( this.mutex ) {
                try { this.mutex.wait();}
                catch (InterruptedException e) {throw new RuntimeException(e);}
            }

            RingBuffer<Event> tRingBuffer = mDisruptor.getRingBuffer();

            for (i = 0; i < cLOOPS; i++) {
                tRingBuffer.publishEvent((event, sequence) -> event.setData(i,mIndex));
            }
            try{ Thread.sleep(5000); }
            catch(InterruptedException e) {}
            System.out.println("Producer " + mIndex + " all done.");
        }
    }

public class MyEventHandler implements EventHandler<Event> {


    @Override
    public void onEvent(Event pEvent, long pSequence, boolean endOfBatch) {
        mTotalSum += pEvent.mValue;
        mEventCount++;


       //ystem.out.println("Event count: " + mEventCount + " thread " + pEvent.getThread() + " cursor: " + mDisruptor.getCursor());

        if  (mEventCount >= cLOOPS * cPRODUCERS) {
            synchronized (mAllDoneMutex) {
                mAllDone = true;
                mAllDoneMutex.notifyAll();
            }
        }
    }
}

public class MyEventFactory implements EventFactory<Event> {
    @Override
    public Event newInstance() {
        return new Event(0,0);
    }
}

public class Event {
    long  mValue;
    long mThread;

    public Event( long pValue, int pThread) {
        mValue = pValue;
        mThread = pThread;
    }
    public void setData(long pValue, long pThread) {
        mValue = pValue;
        mThread = pThread;
    }

    public  long getValue() {
        return mValue;
    }
    public void setValue(long value) {
        mValue = value;
    }

    public  long getThread() {
        return mThread;
    }
    public void setThread(long thread) {
        mThread = thread;
    }
}
}
