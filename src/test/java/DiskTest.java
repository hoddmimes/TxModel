import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

public class DiskTest
{
    enum FileType {Sequential,RandomAccess};
    boolean mForce = true;
    Random mRandom = new Random();
    int mMinSize = 1024 * 20;
    int mMaxSize = 1024 * 20;
    int mCount = 2000;
    FileType mFileType = FileType.RandomAccess;

    public static void main(String[] args) {
        DiskTest dk = new DiskTest();
        dk.parseArguments( args );
        dk.test();
    }

    private void test() {
        long tStartTime = System.nanoTime();
        if (mFileType == FileType.RandomAccess) {
            randomAccessWriteTest();
        } else {
            sequentialWriteTest();
        }
        long tExecTimeUsec = (System.nanoTime() - tStartTime) / 1000L;
        long  writeTimeUsec = tExecTimeUsec / mCount;
        double writesPerSec = (1000000 * (double) mCount) / (double) tExecTimeUsec;
        System.out.println("Type: " + mFileType + " force: " + mForce + "  avg size: " + ((mMaxSize + mMinSize) / 2) + " count:  " + mCount + " write-time: " + writeTimeUsec + "  (usec)   writes/sec: " + writesPerSec);
    }

    private ByteBuffer getBuffer() {
        int tSize = 0;
        if (mMinSize == mMaxSize ) {
            tSize = mMaxSize;
        } else {
            tSize = mMinSize + mRandom.nextInt((mMaxSize - mMinSize));
        }

        ByteBuffer bb = ByteBuffer.allocateDirect(tSize);
        return bb;
    }

    private void sequentialWriteTest() {
        try {
            ByteBuffer byteBuffer = null;
            FileOutputStream fos =  new FileOutputStream(new File("./sequential.log"));
            FileChannel tChannel = fos.getChannel();
            for (int i = 0; i < mCount; i++) {
                byteBuffer = getBuffer();
                tChannel.write(byteBuffer);
                if (mForce) {
                    tChannel.force(true);
                }
            }

        }
        catch( IOException e) {
            e.printStackTrace();
        }
    }

    private void randomAccessWriteTest() {
        try {
            ByteBuffer byteBuffer = null;
            RandomAccessFile tFile = new RandomAccessFile("./random-access.log", "rws");
            FileChannel tChannel = tFile.getChannel();
            for (int i = 0; i < mCount; i++) {
                byteBuffer = getBuffer();
                tChannel.write(byteBuffer);
                if (mForce) {
                    tChannel.force(true);
                }
            }
        }
        catch( IOException e) {
            e.printStackTrace();
        }
    }


    private void parseArguments( String[] args ) {
        int i = 0;
        while(i < args.length) {
            if (args[i].compareTo("-minSize") == 0) {
                mMinSize = Integer.parseInt( args[++i]);
            }
            if (args[i].compareTo("-maxSize") == 0) {
                mMaxSize = Integer.parseInt( args[++i]);
            }
            if (args[i].compareTo("-size") == 0) {
                mMinSize = mMaxSize = Integer.parseInt( args[++i]);
            }
            if (args[i].compareTo("-count") == 0) {
                mCount = Integer.parseInt( args[++i]);
            }
            if (args[i].compareTo("-type") == 0) {
                mFileType = FileType.valueOf( args[++i]);
            }
            if (args[i].compareTo("-force") == 0) {
                mForce = Boolean.parseBoolean( args[++i]);
            }
            i++;
        }
    }
}
