import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hoddmimes.txtest.aux.txlogger.*;
import org.HdrHistogram.Histogram;

import java.text.SimpleDateFormat;
import java.util.List;

public class TestTxlogReply {
    enum RecType {User,Stastics};

    RecType mRecType = RecType.User;

    Histogram mHist = new Histogram(1000000000, 2);
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
           if( args[i].equals( "-rectype" ) ) {
               if (args[++i].compareToIgnoreCase("Statistics") == 0) {
                   mRecType = RecType.Stastics;
               }
           }
           if( args[i].equals( "-service" ) ) {
               mServicename = args[++i];
           }
           i++;
       }
    }

    private void test()
    {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
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

        int tRecType = (mRecType == RecType.User) ? WriteBuffer.FLAG_USER_PAYLOAD : WriteBuffer.FLAG_WRITE_STATISTICS_PAYLOAD;

        while( tReplayer.hasMore( tRecType ) ) {
            TxlogReplyEntryInterface tRec = tReplayer.next(tRecType);

            if (tRec instanceof TxlogReplyEntryMessage) {
                TxlogReplyEntryMessage tMsg = (TxlogReplyEntryMessage) tRec;

                logMsgSeqno = tMsg.getMessageSeqno();
                record_count++;
                String jStr = new String(tMsg.getMsgPayload());
                JsonObject jMsg = JsonParser.parseString(jStr).getAsJsonObject();
                int s = jMsg.get("seqno").getAsInt();
                int d = jMsg.get("time").getAsInt();
                if (d > maxDelta) {
                    maxDelta = d;
                    System.out.println("new max delta: " + d + " seqno: " + (s + 1));
                }
                if (seqno == 0) {
                    seqno = tMsg.getMessageSeqno();
                } else if (tDirection == TxlogReplayer.Direction.Backward) {
                    if ((--seqno) != tMsg.getMessageSeqno()) {
                        System.out.println("Invalid sequence expected: " + seqno + " got " + s + " file: " + tMsg.getFilename());
                    }
                } else {
                    if ((++seqno) != tMsg.getMessageSeqno()) {
                        System.out.println("Invalid sequence expected: " + seqno + " got " + s + " file: " + tMsg.getFilename());
                    }
                }
            } else if (tRec instanceof TxlogReplyEntryStatistics)  {
                TxlogReplyEntryStatistics tStatMsg = (TxlogReplyEntryStatistics) tRec;
                if (tStatMsg.getPreviousWriteTimeUsec() > 0) {
                    mHist.recordValue(tStatMsg.getPreviousWriteTimeUsec());
                }
                //System.out.println("Msgs: " + tStatMsg.getMsgsInBuffer() + " size: " + tStatMsg.getMsgsPayloadSize() + "   prev-write-time: " + tStatMsg.getPreviousWriteTimeUsec() + "  log-time: " + sdf.format( tStatMsg.getLogTime()));
            }
        }
        if (mRecType == RecType.User) {
            System.out.println("All done, number of records: " + record_count + " last message seqno: " + logMsgSeqno);
        } else {
            System.out.println("\n\nStatistic records: " + mHist.getTotalCount() + " mean: " + mHist.getMean() +
                    " max: " + mHist.getMaxValue() + " min: " + mHist.getMinValue() +
                    " 96%: " + mHist.getValueAtPercentile(96) +
                    " 99%: " + mHist.getValueAtPercentile(99) );
        }
    }


}
