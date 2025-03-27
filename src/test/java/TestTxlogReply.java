import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hoddmimes.txtest.aux.txlogger.*;

import java.util.List;

public class TestTxlogReply {

    long startTime;
    String mLogDir = "./logs/";
    String mServicename = "txltest";

    public static void main(String[] args) {
        TestTxlogReply t = new TestTxlogReply();
        t.parseArguments( args );
        t.test();
    }



    private void parseArguments( String[] args ) {
       int i = 0;
       while( i < args.length ) {
           if( args[i].equals( "-logdir" ) ) {
               mLogDir = args[++i];
           }
           if( args[i].equals( "-service" ) ) {
               mServicename = args[++i];
           }
           i++;
       }
    }

    private void test()
    {
        long seqno = 0;
        long logMsgSeqno = 0;
        TxlogReplayer.Direction tDirection = TxlogReplayer.Direction.Forward;

        List<TxLogfile> txl_files = TxlogAux.listTxlogFiles( mLogDir, mServicename );
        for(TxLogfile txf : txl_files) {
            System.out.println(txf.toString());
        }

        if (txl_files.size() > 0) {
            System.out.println("Higiest sequence number found: " + txl_files.get( txl_files.size() - 1).getLastSequenceNumber());
        }


        TxlogReplayer tReplayer = TxLogger.getReplayer(mLogDir, mServicename, TxlogReplayer.Direction.Forward, 0 );
        startTime = System.currentTimeMillis();
        int maxDelta = 0;
        int record_count = 0;



        while( tReplayer.hasMore() ) {
            TxlogReplyEntryMessage tRec = tReplayer.next();

            logMsgSeqno = tRec.getMessageSeqno();
            record_count++;
            String jStr = new String( tRec.getMessageData());
            JsonObject jMsg = JsonParser.parseString(jStr).getAsJsonObject();
            int s = jMsg.get("seqno").getAsInt();
            int d = jMsg.get("time").getAsInt();
            if (d > maxDelta) {
                maxDelta = d;
                System.out.println("new max delta: " + d + " seqno: " + (s+1));
            }
            if (seqno == 0) {
                seqno = tRec.getMessageSeqno();
            } else if (tDirection == TxlogReplayer.Direction.Backward) {
                if ((--seqno)  != tRec.getMessageSeqno()) {
                    System.out.println("Invalid sequence expected: " + seqno + " got " + s + " file: " + tRec.getmFilename());
                }
              } else {
                    if ((++seqno) != tRec.getMessageSeqno()) {
                        System.out.println("Invalid sequence expected: " + seqno + " got " + s + " file: " + tRec.getmFilename());
                    }
                }
        }
        System.out.println("All done, number of records: " + record_count + " last message seqno: " + logMsgSeqno);
    }


}
