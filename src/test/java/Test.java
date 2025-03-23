import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

public class Test
{
    final int mTxToProcessPerThread = 250000;
    final int mProducerThreads = 2;
    final int mTotalTxToProcess = (mTxToProcessPerThread * mProducerThreads);
    AtomicInteger testCounter = new AtomicInteger(0);
    TestExecutor testExecutor;


    public static void main(String[] args) {
        Test t = new Test();
        t.test();
    }
    private void test() {
        testExecutor = new TestExecutor(4);
        long tStartTime = System.currentTimeMillis();
        List<TestProducer> mProducers = new ArrayList();
        for(int i = 0; i < mProducerThreads; i++) {
            TestProducer testProducer = new TestProducer((i+1),mTxToProcessPerThread);
            mProducers.add(testProducer);
            testProducer.start();
        }

        for(int i = 0; i < mProducerThreads; i++) {
           try {
               mProducers.get(i).join();
           }
           catch (InterruptedException e) {
               e.printStackTrace();
           }
        }
        System.out.println(" Tx Count: " + testCounter.get() + " exec-time: " + (System.currentTimeMillis() - tStartTime));

    }

    class Task implements Runnable {
        int resourceId;
        AtomicInteger counterRef;

        public Task(int resourceId, AtomicInteger pCounterRef) {
            this.resourceId = resourceId;
            this.counterRef = pCounterRef;
        }

        @Override
        public void run() {
            counterRef.incrementAndGet();
        }
    }

    void queueTask(Task task) {
        testExecutor.queueRequest(task );
    }

    class TestProducer extends Thread
    {
        private int mThreadIdx;
        private Random mRandom;
        private int mTxCount;

        TestProducer( int pIndex, int pTxCount ) {
            mThreadIdx = pIndex;
            mTxCount = pTxCount;
            mRandom = new Random(System.nanoTime() + mThreadIdx);
        }
        public void run() {
            setName("Thread-" + String.valueOf(mThreadIdx));
            System.out.println("Starting producer Thread-" + String.valueOf(mThreadIdx));
            for (int i = 0; i < mTxCount; i++) {
                queueTask( new Task(mRandom.nextInt(20), testCounter ));
            }
        }
    }



    class TestExecutor {
        private LinkedBlockingDeque<Task> mQueues[];
        private ExecutorService mThreadPool;


    public TestExecutor( int pThreadCount ) {
        mQueues = new LinkedBlockingDeque[pThreadCount];
        ExecutorService mThreadPool = Executors.newFixedThreadPool( pThreadCount );
        for(int i = 0; i < pThreadCount; i++) {
            mQueues[i] = new LinkedBlockingDeque<>();
            mThreadPool.execute( new TestExecutor.ExecThread( i ));
        }
    }

    public void queueRequest(Task tTask) {
        int tQueueIndex = tTask.resourceId % mQueues.length;
        mQueues[tQueueIndex].add( tTask );
    }

    class ExecThread implements Runnable {
        private int mThreadIndex;

        public ExecThread( int pThreadIndex ) {
            mThreadIndex = pThreadIndex;
        }

        @Override
        public void run() {
            System.out.println("Starting executor thread " + mThreadIndex );
            while( true ) {
                try {
                    Task tTask = mQueues[mThreadIndex].take();
                    tTask.run();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}

}
