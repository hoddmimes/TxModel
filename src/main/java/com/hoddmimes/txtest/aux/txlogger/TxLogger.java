package com.hoddmimes.txtest.aux.txlogger;

import com.google.gson.JsonObject;

public class TxLogger
{
        /* Buffer message Entry
    +---------------------------------------------+----------+
    | Message Length                              |  4 bytes |
    +---------------------------------------------+----------+
    | Message entry options  (REPLAY | IGNORE)    |  4 bytes |
    +---------------------------------------------+----------+
    | Message entry data                          |  n bytes |
    +---------------------------------------------+----------+
    | Message Length                              |  4 bytes |
    +---------------------------------------------+----------+
    | Message end MAGIC SIGN                      |  4 bytes |
    +---------------------------------------------+----------+
     */

    public static int RECORD_HEADER_SIZE = 16;

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


    public TxLogger(JsonObject pConfiguration ) {
        if (pConfiguration.has("max_file_size")) {
            mConfigMaxFileSize = pConfiguration.get("max_file_size").getAsLong();
        }
        if (pConfiguration.has("log_files")) {
            mConfigLogFilePattern = pConfiguration.get("log_file").getAsString();
        }
        if (pConfiguration.has("write_align_size")) {
            mConfigAlignSize = pConfiguration.get("write_align_size").getAsInt();
        }
        if (pConfiguration.has("write_buffer_size")) {
            mConfigBufferSize = pConfiguration.get("write_buffer_size").getAsInt();
        }
        this.txlogWriter = new TxlogWriter( this );
    }

    public synchronized TxlogWriter getWriter()
    {
        return this.txlogWriter;
    }

    public TxlogReplayer getReplayer(String pFilenamePattern, int pDirection ) {
        return new TxlogReplayer( pFilenamePattern, pDirection );
    }

}
