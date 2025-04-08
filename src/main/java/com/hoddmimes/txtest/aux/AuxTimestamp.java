package com.hoddmimes.txtest.aux;

import java.util.ArrayList;
import java.util.List;

public class AuxTimestamp
{
    private static final long SEC = 1000000L; // as usec
    private static final long MS = 1000L; // as usec
    private static int STACK_LEVEL = 4;
    private static boolean DISABLED = false;

    private long                 mInitStamp;

    private List<TimestampEntry> mEntries;

    public AuxTimestamp() {
        mEntries = new ArrayList<>();
        mInitStamp = System.nanoTime() / 1000L;
    }

    public AuxTimestamp(String pLabel) {
        mEntries = new ArrayList<>();
        reset( pLabel );
    }

    public static void disable() {
        DISABLED = true;
    }
    public static void enable() {
        DISABLED = false;
    }

    public static void setTraceStackLevel( int pLevel ) {
        STACK_LEVEL = pLevel;
    }

    public static boolean isDisabled() {
        return DISABLED;
    }

    public synchronized  void add(String pLabel ) {
        if (DISABLED) { return; }
        this.add( pLabel,STACK_LEVEL);
    }

    private synchronized void add( String pLabel, int pLevel ) {
        mEntries.add(new TimestampEntry(pLabel, pLevel));
    }

    public synchronized long getTotalTimeUsec() {
        if (DISABLED) { return -1; }
        if (mEntries.size() == 0) {
            return 0L;
        }
        return (mEntries.get( mEntries.size() - 1).mStamp  -  mInitStamp );
    }

    public void reset( String pLabel) {
        if (DISABLED) { return; }
        mEntries.clear();
        mInitStamp = System.nanoTime() / 1000L;
        this.add( pLabel, STACK_LEVEL );
        mEntries.get(0).mStamp = mInitStamp;
    }

    public synchronized long getTotalTimeMs() {
        if (DISABLED) { return -1; }
        if (mEntries.size() == 0) {
            return 0L;
        }
        return ((mEntries.get( mEntries.size() - 1).mStamp  -  mInitStamp ) / MS);
    }

    public String formatUsecTime() {
        if (DISABLED) { return ""; }
        String s;
        long uSec  = getTotalTimeUsec();
        if (uSec == 0) {
            return "000";
        }
        s = String.format("%d (usec)", uSec);
        return s;
    }

    public String formatMsTime() {
        if (DISABLED) { return ""; }
        String s;
        long uSec  = getTotalTimeUsec();
        if (uSec == 0) {
            return "0.000";
        }
        long ms =  (uSec / MS);
        long us = (uSec - (ms * MS));
        s = String.format("%d.%04d (ms)", ms, us);
        return s;
    }

    public String formatTotalTime() {
        if (DISABLED) { return ""; }
        String s;
        long uSec  = getTotalTimeUsec();
        if (uSec == 0) {
            return "0.000";
        }
        long hh =  (uSec / (3600L * SEC));
        long mm =  ((uSec - (hh * 3600L * SEC)) / (60L * SEC));
        long sec =  ((uSec - (hh * 3600L *SEC) - (mm * 60L * SEC)) / SEC);
        long us = (uSec - (hh * 3600L * SEC) - (mm * 60L * SEC) - (sec * SEC));

        if ((hh == 0) && (mm == 0) && (sec == 0)) {
            s = String.valueOf(us);
        } else if ((hh == 0) && (mm == 0) && (sec != 0)) {
            s = String.format("%d.%06d", sec, us);
        } else if ((hh == 0) && (mm != 0) && (sec != 0)) {
            s = String.format("%d:%02d.%06d", mm, sec, us);
        } else {
            s = String.format("%d:%02d:%02d.%06d", hh, mm, sec, us);
        }
        return s;
    }

    public synchronized String toShortString() {
        if (DISABLED) { return "timestamp disabled"; }
        return "\"" + this.mEntries.get(mEntries.size() - 1).mLabel + "\" total exec time : " + this.getTotalTimeUsec() + " usec";
    }

    public synchronized String toString() {
        int tClassLen = 0, tMethodLen = 0;

        if (DISABLED) { return "timestamp disabled"; }

        for( TimestampEntry tse : this.mEntries ) {
               if (tClassLen < tse.getSimpleClass().length()) {tClassLen = tse.getSimpleClass().length();}
               if (tMethodLen < tse.getMethodName().length()) {tMethodLen = tse.getMethodName().length();}
        }
        StringBuilder sb = new StringBuilder("\n");
        long tPrevStamp = mInitStamp;
        String tFormat = "%6d usec %6d usec %-" + String.valueOf( tClassLen + tMethodLen + 3) +"s %s\n";
        for( TimestampEntry tse : this.mEntries ) {
            String s = String.format( tFormat, (tse.mStamp - mInitStamp), (tse.mStamp - tPrevStamp), tse.formatStackTraceEntry( tClassLen, tMethodLen), tse.mLabel);
            sb.append( s );
            tPrevStamp = tse.mStamp;
        }

        sb.append( "Total time "  + formatUsecTime());
        return sb.toString();
    }

    class TimestampEntry
    {
        String              mLabel;
        StackTraceElement   mStack;
        long                mStamp;

        TimestampEntry(String pLabel, int pLevel ) {
            mStamp = System.nanoTime() / 1000L;
            mLabel = pLabel;
            Exception e = new Exception();
            e.fillInStackTrace();
            mStack = (e.getStackTrace().length > pLevel) ? e.getStackTrace()[pLevel] : null;
        }


        String getMethodName() {
            if (mStack == null) {
                return " ";
            }
            return mStack.getMethodName();
        }
        String getSimpleClass() {
            if (mStack == null) {
                return " ";
            }
            String tClassName = mStack.getClassName();
            if (mStack.getClassName().indexOf(".") >= 0) {
                tClassName = tClassName.substring( tClassName.lastIndexOf(".") + 1);
            }
            return tClassName;
        }
        String formatStackTraceEntry( int pClassLen, int pMethodLen) {
            if (mStack == null) {
                return null;
            }

            String tFormat = "%-" + String.valueOf( pClassLen) + "s %-" + String.valueOf( pMethodLen ) + "s %5d";

            String tStr =  String.format(tFormat, getSimpleClass(), mStack.getMethodName(), mStack.getLineNumber());
            return tStr;
        }
    }


    private void test02() {
        test01();
    }
    private void test01() {
        test();
    }
    private void test() {
        AuxTimestamp ts = new AuxTimestamp("Start");

        AuxTimestamp.disable();

        for (int i = 0; i < 10000; i++) {
            ts.reset("Start");
            for (int j = 0; j < 10; j++) {
                ts.add("TS-" + String.valueOf(j));
            }
            if (i > 5000) {
                System.out.println(ts.toString());
                try {
                    Thread.sleep(3000L);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    public static void main( String[] args ) {
        AuxTimestamp ts = new AuxTimestamp();


        ts.add("TS-2");
        for (int i = 0; i < 1000; i++) {
            long x = System.nanoTime();
        }
        ts.add("TS-3");
        ts.add("Send Processed Request Message Response TimestampCntx  foo");
        for (int i = 0; i < 5000000; i++) {
            long x = System.nanoTime();
        }
        ts.add("TS-4");


        System.out.println("AuxTime: " + ts.formatTotalTime() + " usec: " + ts.getTotalTimeUsec());
        System.out.println(ts.toString());
    }

}
