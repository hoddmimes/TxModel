import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hoddmimes.txtest.aux.txlogger.TxLogger;
import com.hoddmimes.txtest.aux.txlogger.TxlogReplayRecord;
import com.hoddmimes.txtest.aux.txlogger.TxlogReplayer;

public class TestTxlogReply {

    long startTime;

    public static void main(String[] args) {
        TestTxlogReply t = new TestTxlogReply();
        t.test();
    }

    private JsonObject createConfiguration() {
        JsonObject jConfig = new JsonObject();
        jConfig.addProperty("max_file_size", 100 * 1000 * 1000);
        jConfig.addProperty("log_files", "txlog_#sequence#.log");
        jConfig.addProperty("write_align_size", 512);
        jConfig.addProperty("write_buffer_size", 8192*3);
        jConfig.addProperty("write_holdback", 30);
        return jConfig;
    }

    private void test()
    {
        int seqno = 0;
        long logMsgSeqno = 0;
        int direction = TxlogReplayer.BACKWARD;

        JsonObject tConfig = createConfiguration();
        TxLogger txl = new TxLogger( tConfig );
        TxlogReplayer tReplayer = txl.getReplayer("./logs/txlog_*.log", direction );
        startTime = System.currentTimeMillis();
        int maxDelta = 0;
        int record_count = 0;

        TxlogReplayRecord tRec = tReplayer.next();
        while( tRec != null) {
            if (tRec != null) {
                logMsgSeqno = tRec.getMsgSeqno();
                record_count++;
                String jStr = new String( tRec.getData());
                JsonObject jMsg = JsonParser.parseString(jStr).getAsJsonObject();
                int s = jMsg.get("seqno").getAsInt();
                int d = jMsg.get("time").getAsInt();
                if (d > maxDelta) {
                    maxDelta = d;
                    System.out.println("new max delta: " + d + " seqno: " + (s+1));
                }
                if (seqno == 0) {
                    seqno = s;
                } else if (direction == TxlogReplayer.BACKWARD) {
                    if ((--seqno)  != s) {
                        System.out.println("Invalid sequence expected: " + seqno + " got " + s);
                    }
                } else {
                    if ((++seqno) != s) {
                        System.out.println("Invalid sequence expected: " + seqno + " got " + s);
                    }
                }
            }
            tRec = tReplayer.next();
        }
        System.out.println("All done, number of records: " + record_count + " last message seqno: " + logMsgSeqno);
    }


}
