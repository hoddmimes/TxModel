import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hoddmimes.txtest.aux.txlogger.TxLogfile;
import com.hoddmimes.txtest.aux.txlogger.TxLogger;
import com.hoddmimes.txtest.aux.txlogger.TxlogReplayRecord;
import com.hoddmimes.txtest.aux.txlogger.TxlogReplayer;

import java.util.List;

public class TestTxlogReply {

    long startTime;
    String mLogfilePattern = "./logs/txlog_#sequence#.log";

    public static void main(String[] args) {
        TestTxlogReply t = new TestTxlogReply();
        if ((args.length == 2) && (args[0].compareTo("-logs") == 0)) {
            t.mLogfilePattern = args[1];
        }
        t.test();
    }

    private JsonObject createConfiguration( String pLogfilePattern) {
        JsonObject jConfig = new JsonObject();
        jConfig.addProperty("max_file_size", 100 * 1000 * 1000);
        jConfig.addProperty("log_files", pLogfilePattern);
        jConfig.addProperty("write_align_size", 512);
        jConfig.addProperty("write_buffer_size", 8192*3);
        jConfig.addProperty("write_holdback", 30);
        return jConfig;
    }

    private void test()
    {
        long seqno = 0;
        long logMsgSeqno = 0;
        int direction = TxlogReplayer.FORWARD;

        JsonObject tConfig = createConfiguration(mLogfilePattern);
        TxLogger txl = new TxLogger( tConfig );
        List<TxLogfile> txl_files = TxLogger.listPatternTxLogfiles( tConfig.get("log_files").getAsString() );
        for(TxLogfile txf : txl_files) {
            System.out.println(txf.toString());
        }

        System.out.println("Start reading, current seqno: " + txl.getServerMessageSeqno());
        TxlogReplayer tReplayer = txl.getReplayer("./logs/txlog_*.log", direction, 0 );
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
                    seqno = tRec.getMsgSeqno();
                } else if (direction == TxlogReplayer.BACKWARD) {
                    if ((--seqno)  != tRec.getMsgSeqno()) {
                        System.out.println("Invalid sequence expected: " + seqno + " got " + s + " file: " + tRec.getFilename());
                    }
                } else {
                    if ((++seqno) != tRec.getMsgSeqno()) {
                        System.out.println("Invalid sequence expected: " + seqno + " got " + s + " file: " + tRec.getFilename());
                    }
                }
            }
            tRec = tReplayer.next();
        }
        System.out.println("All done, number of records: " + record_count + " last message seqno: " + logMsgSeqno);
    }


}
