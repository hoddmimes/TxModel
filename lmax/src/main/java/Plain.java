import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

public class Plain
{
    private static int cLOOPS = 100000;
    private static int cPRODUCERS = 5;

    long mTotalSum;

    LinkedBlockingDeque<Event> mQueue;
    public static void main(String[] args) {
        Plain t = new Plain();
        t.test();
    }

    private void test() {
        Producer producers[] = new Producer[cPRODUCERS];

        mTotalSum = 0;
        mQueue = new LinkedBlockingDeque<>();
        Consumer consumer = new Consumer();
        consumer.start();
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


        synchronized( consumer ) {
            try{consumer.join();}
            catch( InterruptedException e) {}
        }
        long tExecTimeUsec = (System.nanoTime() - tStartTime) / 1000L;
        System.out.println("TotalSum: " + mTotalSum + " exec time: " + tExecTimeUsec + " usec.");



    }

    class Consumer extends Thread
    {
        Object mutex = new Object();
        int mEventCount = 0;

        public void run() {
            System.out.println("Starting consumer " );
            List<Event> tEventList = new ArrayList<>(30);
            try {
                while (mEventCount < cLOOPS * cPRODUCERS) {
                    Event tEvent = mQueue.take();
                    mTotalSum += tEvent.mValue;
                    mEventCount++;

                    if (!mQueue.isEmpty()) {
                        tEventList.clear();
                        mQueue.drainTo(tEventList);
                        for (Event evt : tEventList) {
                            mTotalSum += evt.mValue;
                            mEventCount++;
                        }
                    }
                }
            }
            catch( InterruptedException e ) {}
            System.out.println("Event count: " + mEventCount  );
        }
    }

    class Producer extends Thread
    {
        Object mutex = new Object();  // To synchronize producer and consumer threads.
        int mIndex;

        Producer( int pIndex ) {
            mIndex = pIndex;
        }
        public void run() {
            System.out.println("Starting producer " + mIndex );
            synchronized ( this.mutex ) {
                try { this.mutex.wait();}
                catch (InterruptedException e) {throw new RuntimeException(e);}
            }

            for (int i = 0; i < cLOOPS; i++) {
                mQueue.add( new Event(i,mIndex));
            }
        }
    }

    class Event {
        long  mValue;
        long mThread;

        Event( long pValue, int pThread) {
            mValue = pValue;
            mThread = pThread;
        }
    }
}
