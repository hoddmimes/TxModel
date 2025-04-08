import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class AllocationTest
{
    static int StaticIndex = 37;

    boolean mStatic = false;
    int mThreads = 1;
    int mActiveMemObject = 5000;
    int mMinSize = 64;
    int mMaxSize = 256;
    long mIterations = 1000000000;
    AtomicLong mCount = new AtomicLong(0);

    int mActivations;


    public static void main(String[] args) {
        AllocationTest allocationTest = new AllocationTest();
        allocationTest.parseArguments( args );
        allocationTest.test();
    }

    private void parseArguments( String[] args) {
        int i = 0;
        while( i < args.length ) {
            if (args[i].compareTo("-static") == 0) {
                mStatic = Boolean.parseBoolean(args[++i]);
            }
            if (args[i].compareTo("-minSize") == 0) {
                mMinSize = Integer.parseInt(args[++i]);
            }
            if (args[i].compareTo("-maxSize") == 0) {
                mMaxSize = Integer.parseInt(args[++i]);
            }
            if (args[i].compareTo("-iterations") == 0) {
                mIterations = Long.parseLong(args[++i]);
            }
            i++;
        }
    }

    private void test() {
      MemThread[] tThreads = new MemThread[ mThreads];
        long tStartTime = System.nanoTime();

        for (int i = 0; i < mThreads; i++) {
            tThreads[i] = new MemThread((i+1));
            tThreads[i].start();
        }
        for (int i = 0; i < mThreads; i++) {
            try {tThreads[i].join();}
            catch( InterruptedException e) {}
        }
        long tExecTimeUsec = (System.nanoTime() - tStartTime) / 1000L;
        long tExecTimeMs = (System.nanoTime() - tStartTime) / 1000000L;
        System.out.println("Static: " + mStatic + " exec-time: " + tExecTimeUsec + " usec (" + tExecTimeMs + " ms.) Iterations: " + mIterations + " min-size: " + mMinSize + " max-size: " + mMaxSize );

    }

    class MemThread extends Thread {
        HashMap<Integer, MemObject> memObjects;
        Random mRandom = new Random();
        int mThreadIndex;
        MemObject mStatisObject;

        MemThread( int pThreadIndex) {
            mThreadIndex = pThreadIndex;
            memObjects = new HashMap<>( mActiveMemObject * 2);
            if (mStatic) {
                mStatisObject = new MemObject(StaticIndex, ((mMaxSize - mMinSize) + mMinSize));
                memObjects.put(StaticIndex, mStatisObject);
            }
        }

        public void run() {
            int mLoopCount = 0;

            while ( mCount.incrementAndGet() < mIterations) {
                mLoopCount++;
                if (mStatic) {
                    mRandom.nextInt(mActiveMemObject);
                    MemObject mo = memObjects.get( StaticIndex );
                    mo.increment(1L);
                } else {
                    int pIndex = mRandom.nextInt(mActiveMemObject);
                    MemObject mo = new MemObject(pIndex, mMinSize + mRandom.nextInt((mMaxSize - mMinSize)));
                    mo.increment(1L);
                    memObjects.put( pIndex, mo);
                }
            }
            System.out.println("Thread index: " + mThreadIndex + " completed, interations: " + mLoopCount);
        }
    }


    class MemObject {
        int     mId;
        byte[]  mData;
        long    mValue;

        MemObject( int pId, int pSize ) {
            mId = mId;
            mData = new byte[pSize];
        }

        void increment( long pValue ) {
            mValue += pValue;
        }
    }

}
