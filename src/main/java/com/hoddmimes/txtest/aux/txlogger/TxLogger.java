package com.hoddmimes.txtest.aux.txlogger;

import com.google.gson.JsonObject;
import com.hoddmimes.txtest.server.ServerMessageSeqnoInterface;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TxLogger implements ServerMessageSeqnoInterface
{
    public static String SEQUENCE_DATETIME = "#datetime#";
    public static String SEQUENCE_SEQUENCE = "#sequence#";

        /* Buffer message Entry
    +---------------------------------------------+----------+
    | Message Length                              |  4 bytes |
    +---------------------------------------------+----------+
    | Log entry sequence number                   |  8 bytes |
    +---------------------------------------------+----------+
    | Message entry data                          |  n bytes |
    +---------------------------------------------+----------+
    | Message Length                              |  4 bytes |
    +---------------------------------------------+----------+
    | Message end MAGIC SIGN                      |  4 bytes |
    +---------------------------------------------+----------+

        The log-entry-sequence-number is sequentially incremented for each log entry written to the logfile.
        For filler/align entries the sequence number will be 0 (zero) and should not be replayed

     */

    public static int RECORD_HEADER_SIZE = 20;

    public static int MAGIC_END = 0x504F4203;

    public static int REPLAY_OPTION_REPLAY = 1;
    public static int REPLAY_OPTION_IGNORE = 2;


    /*
      TxLogger configuration parameters with default values
     */

    long    mConfigMaxFileSize = (1024 * 1024 * 10);       // !0 Mb default size
    String  mConfigLogFilePattern = "./logs/TxLog-#datetime#.log";
    int     mConfigAlignSize = 512;
    int     mConfigBufferSize = 4096 * 4;

    volatile TxlogWriter txlogWriter = null;

    AtomicLong  mTxMessageSeqno = new AtomicLong(0L);

    public TxLogger(JsonObject pConfiguration ) {
        if (pConfiguration.has("max_file_size")) {
            mConfigMaxFileSize = pConfiguration.get("max_file_size").getAsLong();
        }
        if (pConfiguration.has("log_files")) {
            mConfigLogFilePattern = pConfiguration.get("log_files").getAsString();
        }
        if (pConfiguration.has("write_align_size")) {
            mConfigAlignSize = pConfiguration.get("write_align_size").getAsInt();
        }
        if (pConfiguration.has("write_buffer_size")) {
            mConfigBufferSize = pConfiguration.get("write_buffer_size").getAsInt();
        }

        if ((!mConfigLogFilePattern.contains("#datetime#")) && (!mConfigLogFilePattern.contains(SEQUENCE_SEQUENCE))) {
            throw new RuntimeException("Invalid logfile patterna, must contain sequence pattern \"#datetime#\" or \"#sequence#\"");
        }

        findTxMessageSeqno();


    }

    public synchronized TxlogWriter getWriter()
    {
        if (txlogWriter == null) {
            txlogWriter = new TxlogWriter(this);
        }
        return txlogWriter;
    }

    public TxlogReplayer getReplayer(String pFilenamePattern, int pDirection ) {
        return new TxlogReplayer( pFilenamePattern, pDirection );
    }

    public TxlogReplayer getReplayer(String pFilenamePattern, int pDirection, long pFromMessageSeqno ) {
        return new TxlogReplayer( pFilenamePattern, pDirection, pFromMessageSeqno );
    }

    public String getLogFilePattern() {
        return mConfigLogFilePattern;
    }

    public static List<TxLogfile> listPatternTxLogfiles( String pLogfilePattern ) {
        String tWildcardPattern = pLogfilePattern;

        if (pLogfilePattern.contains(TxLogger.SEQUENCE_SEQUENCE)) {
            tWildcardPattern = pLogfilePattern.replace(TxLogger.SEQUENCE_SEQUENCE,"*");
        }
        if (pLogfilePattern.contains(TxLogger.SEQUENCE_DATETIME)) {
            tWildcardPattern = pLogfilePattern.replace(TxLogger.SEQUENCE_DATETIME,"*");
        }
        return listWildcardTxLogfiles( tWildcardPattern );
    }
    public static List<TxLogfile> listWildcardTxLogfiles( String pLogWildcardPattern ) {
            FileUtilParse fnp = new FileUtilParse(pLogWildcardPattern);
            List<String> tFilenames = fnp.listWildcardFiles();

            List<TxLogfile> tTxLogfiles = new ArrayList<>();

            for( String tFilename : tFilenames) {
                    tTxLogfiles.add(new TxLogfile(tFilename));
            }
            return tTxLogfiles;
    }



    private void findTxMessageSeqno() {
        List<TxLogfile> tTxLogfiles = TxLogger.listPatternTxLogfiles(this.mConfigLogFilePattern);
        if (tTxLogfiles.size() == 0) {
            mTxMessageSeqno.set(0L);
        } else {
            mTxMessageSeqno.set(tTxLogfiles.get( tTxLogfiles.size() - 1).getLastSequenceNumber());
        }
    }

    private List<String> findTxLogFile() {
        FileUtilParse fnp = new FileUtilParse(this.mConfigLogFilePattern);
        List<String> tAllFiles = fnp.listFilenames(true);
        List<String> tFiles = new ArrayList<>();

        if (this.mConfigLogFilePattern.contains(TxLogger.SEQUENCE_DATETIME)) {
            String tPrefix = fnp.getName().substring(0, fnp.getName().length() - SEQUENCE_DATETIME.length());
            Pattern tPattern = Pattern.compile(tPrefix + "\\d{6}_\\d{6}_\\d{3}\\." + fnp.getExtention());
            for (String fn : tAllFiles) {
                Matcher m = tPattern.matcher(fn);
                if (m.find()) {
                    tFiles.add(fn);
                }
            }
        }
        else if (this.mConfigLogFilePattern.contains(TxLogger.SEQUENCE_SEQUENCE)) {
            String tPrefix = fnp.getName().substring(0, fnp.getName().length() - SEQUENCE_SEQUENCE.length());
            Pattern tPattern = Pattern.compile(tPrefix + "\\d+\\." + fnp.getExtention());
            for (String fn : tAllFiles) {
                Matcher m = tPattern.matcher(fn);
                if (m.find()) {
                    tFiles.add(fn);
                }
            }

        } else {
            for (String fn : tAllFiles) {
                if (fn.contains(fnp.getName() + "." + fnp.getExtention())) {
                    tFiles.add(fn);
                }
            }
        }
        return tFiles;
    }



    @Override
    public long getServerMessageSeqno() {
        return this.mTxMessageSeqno.get();
    }

    public long incrementAndGetServerMessageSeqno() {
        return this.mTxMessageSeqno.incrementAndGet();
    }


}
