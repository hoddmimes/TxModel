import com.google.gson.JsonObject;
import com.hoddmimes.txtest.aux.txlogger.TxLogger;
import com.hoddmimes.txtest.aux.txlogger.TxlogWriteCallback;
import com.hoddmimes.txtest.aux.txlogger.TxlogWriter;

import java.nio.charset.StandardCharsets;
import java.util.Random;

public class TestTxlogWrite implements TxlogWriteCallback {

    private static int LOOP_COUNT = 300000;

    long startTime;
    long lastNanoTime = 0;

    public static void main(String[] args) {
        TestTxlogWrite t = new TestTxlogWrite();
        t.test();
    }

    private JsonObject createConfiguration() {
        JsonObject jConfig = new JsonObject();
        jConfig.addProperty("max_file_size", 10 * 1000 * 1000);
        jConfig.addProperty("log_files", "txlog.log");
        jConfig.addProperty("write_align_size", 512);
        jConfig.addProperty("write_buffer_size", 8192*3);

        return jConfig;
    }


    private byte[] createMessage( int pSeqNo) {
        long tNow = System.nanoTime() / 1000L;
        long tDelta = (lastNanoTime == 0) ? 0 : (tNow - lastNanoTime);
        lastNanoTime = tNow;
        String str = "{\"seqno\" : " + String.valueOf( pSeqNo ) + ", \"time\" : " + tDelta + "}";
        return str.getBytes(StandardCharsets.UTF_8);
    }

    private void test()
    {
        Random rnd = new Random();
        JsonObject tConfig = createConfiguration();
        TxLogger txl = new TxLogger( tConfig );
        TxlogWriter txw = txl.getWriter();
        startTime = System.currentTimeMillis();
        for( int i = 0; i < LOOP_COUNT; i++ ) {
            if (i == (LOOP_COUNT - 1)) {
                txw.queueMessage(createMessage((i+1)), this, null);
            } else {
                txw.queueMessage(createMessage((i+1)));
            }
        }
        synchronized ( this ) {
            try {
                this.wait();
                System.out.println("All done!!!");
                System.exit(0);
            } catch (InterruptedException e) {
            }
        }
    }

    @Override
    public void txlogWriteComplete(Object mCallbackParameter) {
        System.out.println("Exec time: " + (System.currentTimeMillis() - startTime));
        synchronized (this) {
            this.notifyAll();
        }
    }
}
