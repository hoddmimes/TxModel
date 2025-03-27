package com.hoddmimes.txtest.aux.txlogger;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

public class TxlogAux {

    public static List<String> listFiles(String pLogDir, String pTxlService) {
        File tDir = new File(pLogDir);
        final Pattern tFilenamePattern = Pattern.compile(pTxlService + "-\\d{6}_\\d{9}" + "\\.txl");

        List<String> tFileNames = new ArrayList<>();
        if (!tDir.isDirectory()) {
            return tFileNames;
        }

        File[] tFiles = tDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return tFilenamePattern.matcher(name).matches();
            }
        });

        if (tFiles != null) {
            for (File tFile : tFiles) {
                if (tFile.isFile()) {
                    tFileNames.add(tFile.getAbsolutePath());
                }
            }
        }
        return tFileNames;
    }

    public static List<TxLogfile> listTxlogFiles(String pLogDir, String pTxlService) {
        List<String> tFileNames = listFiles(pLogDir, pTxlService);
        List<TxLogfile> tTxlogFiles = new ArrayList<>();
        for (String tFilename : tFileNames) {
            tTxlogFiles.add(new TxLogfile(tFilename));
        }
        Collections.sort(tTxlogFiles);
        return tTxlogFiles;
    }

    static long getMessageSeqnoInTxlog(String pLogDir, String pTxlService) {
        List<String> tFileNames = listFiles(pLogDir, pTxlService);
        if (tFileNames.size() == 0) {
            return 0L;
        }
        List<TxLogfile> tTxlogFiles = new ArrayList<>();
        for (String tFilename : tFileNames) {
            tTxlogFiles.add(new TxLogfile(tFilename));
        }
        if (tTxlogFiles.size() == 0) {
            return 0L;
        }


        Collections.sort(tTxlogFiles);
        return tTxlogFiles.get( tTxlogFiles.size() - 1).getLastSequenceNumber();
    }



    static long alignToEndOfTxlogFile(RandomAccessFile pRandomAccessFile) throws IOException {
        FileChannel tChannel = pRandomAccessFile.getChannel();

        if (tChannel.size() < WriteBuffer.TXLOG_HEADER_SIZE) {
            return 0L;
        }

        long tPos = tChannel.size() - Integer.BYTES;
        while (tPos > 0) {
            tChannel.position(tPos);
            if (pRandomAccessFile.readInt() == WriteBuffer.MAGIC_END_MARKER) {
                return (tPos + Integer.BYTES);
            } else {
                tPos -= 1; // Backup one byte
            }
        }
        return 0L;
    }

    public static  int bytesToInt(byte[] pBuffer, int pOffset )
    {
        int tValue = 0;
        tValue += ((pBuffer[ pOffset + 0] & 0xff) << 24);
        tValue += ((pBuffer[ pOffset + 1] & 0xff) << 16);
        tValue += ((pBuffer[ pOffset + 2] & 0xff) << 8);
        tValue += ((pBuffer[ pOffset + 3] & 0xff) << 0);
        return tValue;
    }

    public static  long bytesToLong(byte[] pBuffer, int pOffset )
    {
        long tValue = 0;
        tValue += (long) ((long)(pBuffer[ pOffset + 0] & 0xff) << 56);
        tValue += (long) ((long)(pBuffer[ pOffset + 1] & 0xff) << 48);
        tValue += (long) ((long)(pBuffer[ pOffset + 2] & 0xff) << 40);
        tValue += (long) ((long)(pBuffer[ pOffset + 3] & 0xff) << 32);
        tValue += (long) ((long)(pBuffer[ pOffset + 4] & 0xff) << 24);
        tValue += (long) ((long)(pBuffer[ pOffset + 5] & 0xff) << 16);
        tValue += (long) ((long)(pBuffer[ pOffset + 6] & 0xff) << 8);
        tValue += (long) ((long)(pBuffer[ pOffset + 7] & 0xff) << 0);
        return tValue;
    }

    public static void longToBytes( long pValue, byte[] pBuffer, int
            pOffset )
    {
        pBuffer[ pOffset + 0] = (byte) (pValue >>> 56);
        pBuffer[ pOffset + 1] = (byte) (pValue >>> 48);
        pBuffer[ pOffset + 2] = (byte) (pValue >>> 40);
        pBuffer[ pOffset + 3] = (byte) (pValue >>> 32);
        pBuffer[ pOffset + 4] = (byte) (pValue >>> 24);
        pBuffer[ pOffset + 5] = (byte) (pValue >>> 16);
        pBuffer[ pOffset + 6] = (byte) (pValue >>> 8);
        pBuffer[ pOffset + 7] = (byte) (pValue >>> 0);
    }

    public static void intToBytes( int pValue, byte[] pBuffer, int pOffset )
    {
        pBuffer[ pOffset + 0] = (byte) (pValue >>> 24);
        pBuffer[ pOffset + 1] = (byte) (pValue >>> 16);
        pBuffer[ pOffset + 2] = (byte) (pValue >>> 8);
        pBuffer[ pOffset + 3] = (byte) (pValue >>> 0);
    }

}



