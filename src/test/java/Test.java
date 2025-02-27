import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;


public class Test
{
    long mPos;
    RandomAccessFile mFile;
    FileChannel mFileChannel;

    public static void main(String[] args) {
        Test t = new Test();
        t.test();
    }

    private void updpos( int tSize) throws IOException {
        mPos += tSize;
        mFileChannel.position(mPos);
    }

    private void test() {
        try {
            mFile = new RandomAccessFile("./txlog_1.log", "r");
            mFileChannel = mFile.getChannel();
            mPos = mFileChannel.size();


            updpos(-4);
            int ms = mFile.readInt();  // Read end mark
            System.out.println("Magical Sign: " + Integer.toHexString( ms ));

            updpos(-4);
            int s2 = mFile.readInt();   // Read Size
            System.out.println("Length 2: " +  s2 );


            byte[] tData = new byte[s2];
            updpos( -s2 );
            mFile.read(tData);
            // Read Data
            updpos( -4 );
            int ro = mFile.readInt(); // Read replay option
            System.out.println("Replay Option: " + Integer.toHexString( ro ));


            updpos( -4 );
            int s1 = mFile.readInt();// Read replay option
            System.out.println("Length 1: " +  s1 );




        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
