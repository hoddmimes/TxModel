import com.google.gson.JsonObject;
import com.hoddmimes.txtest.aux.txlogger.*;
import com.hoddmimes.txtest.generated.fe.messages.UpdateMessage;
import com.hoddmimes.txtest.server.Asset;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestTxlogCompare {

    String mLogDir01, mLogDir02;
    String mService = "tx-test";

    public static void main(String[] args) {
        TestTxlogCompare t = new TestTxlogCompare();
        t.parseArguments(args);
        t.test();
    }

    private void parseArguments(String[] args) {
        int i = 0;
        while (i < args.length) {
            if (args[i].contains("-logs01")) {
                mLogDir01 = args[++i];
            }
            if (args[i].contains("-logs02")) {
                mLogDir02 = args[++i];
            }
            i++;
        }
        if ((mLogDir01 == null) || (mLogDir02 == null)) {
            System.err.println("Usage: java TestTxlogReplyCompare -logs01 <logdir> -logs02 <logdir>");
            System.exit(1);
        }
    }


    private void test() {
        Replayer replayer1 = new Replayer(mLogDir01, mService, TxlogReplayer.Direction.Forward, 0);
        Replayer replayer2 = new Replayer(mLogDir02, mService, TxlogReplayer.Direction.Forward, 0);

        List<TxLogfile> txl_files1 = TxlogAux.listTxlogFiles(mLogDir01, mService);
        List<TxLogfile> txl_files2 = TxlogAux.listTxlogFiles(mLogDir02, mService);

        if (txl_files1.size() != txl_files2.size()) {
            System.out.println("Number of logfiles are different " + mLogDir01 + " : " + txl_files1.size() + "   " + mLogDir02 + " : " + txl_files2.size());
            System.exit(0);
        }

        for (int i = 0; i < txl_files1.size(); i++) {
            TxLogfile f1 = txl_files1.get(i);
            TxLogfile f2 = txl_files2.get(i);

            if (f1.compareTo(f2) != 0) {
                System.out.println("Warning!!! Logfiles " + i + " are different.\n " + f1.toString() + "\n" + f2.toString());
            }
        }
        replayer1.rebuild();
        System.out.println(mLogDir01 + "  " + replayer1.toString());
        replayer2.rebuild();
        System.out.println(mLogDir02 + "  " + replayer2.toString());


        System.out.println("Starting to compare contents...");
        TxlogReplayer txlr1 = TxLogger.getReplayer( mLogDir01, mService, TxlogReplayer.Direction.Forward, 0);
        TxlogReplayer txlr2 =  TxLogger.getReplayer(mLogDir02, mService, TxlogReplayer.Direction.Forward, 0);


        while (true) {
            TxlogReplyEntryMessage r1 = txlr1.next();
            TxlogReplyEntryMessage r2 = txlr2.next();

            if (r1 == null && r2 == null) {
                System.out.println("End of replay.");
                break;
            }
            if (r1 == null || r2 == null) {
                if (txlr1 == null) {
                    System.out.println(mLogDir01 + " ended before " + mLogDir02 + " 02-rec: " + r2.toString());
                } else if (txlr2 == null) {
                    System.out.println(mLogDir02 + " ended before " + mLogDir01 + " 02-rec: " + r1.toString());
                    break;
                }
            }


            UpdateMessage updmsg1 = new UpdateMessage(r1.getMsgPayload());
            UpdateMessage updmsg2 = new UpdateMessage(r2.getMsgPayload());

            if (updmsg1.getValue() != updmsg2.getValue()) {
                System.out.println("Mismatch 01-seqnumber: " + r1.getMessageSeqno() + " 02-seqnumber: " + r2.getMessageSeqno() + "\n" +
                        r1.getFilename() + "   " + r2.getFilename());
            }

            if (r1.getTxid() != r2.getTxid()) {
                System.out.println("Mismatch Txid 01-seqnumber: " + r1.getMessageSeqno() + " 02-seqnumber: " + r2.getMessageSeqno() + "\n" +
                        r1.getFilename() + "   " + r2.getFilename());
            }


        }
        System.out.println("Comparency completed");
    }


    class Replayer {
        private JsonObject jConfiguration;
        private TxLogger txl;
        private TxlogReplayer txlogReplayer;
        private AssetController assetController;


        Replayer(String pLogDir, String pServicename,TxlogReplayer.Direction pDirection, long pFromSeqno) {
            assetController = new AssetController();
            txlogReplayer = TxLogger.getReplayer(pLogDir,  pServicename, pDirection, pFromSeqno);
        }

        void rebuild() {
            while( txlogReplayer.hasMore()) {
                TxlogReplyEntryMessage tlrm = txlogReplayer.next();
                if (tlrm != null) {
                    UpdateMessage updmsg = new UpdateMessage(tlrm.getMsgPayload());
                    assetController.update(updmsg);
                }
            }
        }

        public String toString() {
            return assetController.toString();
        }


        class AssetController {
            int mUpdates = 0;
            Map<Integer, Asset> mAssets;

            AssetController() {
                mAssets = new HashMap<>();
            }

            public String toString() {
                int tCheckSum = mAssets.values().stream().mapToInt(u -> u.getValue()).sum();
                return "AssetController updates: " + mUpdates + " assets:  " + mAssets.size() + " chksum: " + tCheckSum;
            }


            void update(UpdateMessage pUpdateMsg) {
                Asset asset = mAssets.get(pUpdateMsg.getAssetId());
                if (asset == null) {

                    asset = new Asset(pUpdateMsg.getAssetId());
                    mAssets.put(pUpdateMsg.getAssetId(), asset);
                }
                mUpdates++;
                asset.update(pUpdateMsg.getValue());
            }
        }
    }
}
