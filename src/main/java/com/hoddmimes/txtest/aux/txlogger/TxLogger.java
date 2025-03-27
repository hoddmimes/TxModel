package com.hoddmimes.txtest.aux.txlogger;

import com.google.gson.JsonObject;

public class TxLogger {

    public static TxlogWriter getWriter(String pLogDir, String pServiceName, JsonObject jTxConfig) {
        TxlogConfig txlogConfig = new TxlogConfig();
        if (jTxConfig.has("write_buffer_size")) {
            txlogConfig.setWriteBufferSize(jTxConfig.get("write_buffer_size").getAsInt());
        }
        if (jTxConfig.has("write_align_size")) {
            txlogConfig.setWriteBufferAlignSize(jTxConfig.get("write_align_size").getAsInt());
        }
        if (jTxConfig.has("max_file_size")) {
            txlogConfig.setLogfileMaxSize(jTxConfig.get("max_file_size").getAsInt());
        }
        return new TxlogWriter(pLogDir,pServiceName, txlogConfig);
    }

    public static TxlogReplayer getReplayer(String pLogDir, String pServiceName, TxlogReplayer.Direction pDirection) {
        return new TxlogReplayer(pLogDir, pServiceName, pDirection);
    }

    public static TxlogReplayer getReplayer(String pLogDir, String pServiceName, TxlogReplayer.Direction pDirection, long pFromSeqno) {
        return new TxlogReplayer(pLogDir, pServiceName, pDirection, pFromSeqno);
    }

}
