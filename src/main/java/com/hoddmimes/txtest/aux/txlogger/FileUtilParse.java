package com.hoddmimes.txtest.aux.txlogger;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

public class FileUtilParse {
    File tFile;

    public FileUtilParse(String pFilename) {
        if (!pFilename.contains(File.separator)) {
            String cwd = Path.of("").toAbsolutePath().toString();
            tFile = new File(cwd + File.separator + pFilename);
        } else {
            tFile = new File(pFilename);
        }
    }

    public String getFullname() {
        return tFile.getAbsolutePath();
    }

    public String getExtention() {
        String tName = tFile.getName();
        int lastIndexOf = tName.lastIndexOf(".");
        if (lastIndexOf == -1) {
            return ""; // empty extension
        }
        return tName.substring(lastIndexOf + 1);
    }

    public String getName() {
        String tName = tFile.getName();
        int lastIndexOf = tName.lastIndexOf(".");
        if (lastIndexOf == -1) {
            return tName; // empty extension
        }
        return tName.substring(0, lastIndexOf);
    }

    public String getDir() {
        return tFile.getAbsoluteFile().getParentFile().toString() + File.separator;
    }

    public List<String> listFilenames(boolean pWithExtensions) {
        File tDir = new File(this.getDir());
        File[] tFiles = tDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                if (pWithExtensions) {
                    return name.endsWith(FileUtilParse.this.getExtention());
                }
                return true;
            }
        });
        ArrayList<String> tFilenames = new ArrayList<>();
        for (int i = 0; i < tFiles.length; i++) {
            tFilenames.add(tFiles[i].getName());
        }
        return tFilenames;
    }

    public List<String> listWildcardFiles() {
        Pattern tWldcrdPattern = Pattern.compile( this.getName().replace("?",".?").replace("*",".*?") +
                                                   "\\." + this.getExtention().replace("?",".?").replace("*",".*?"));
        File tDir = new File(this.getDir());
        File[] tFiles = tDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                    return tWldcrdPattern.matcher(name).matches();
                }
        });

        Arrays.sort( tFiles, new FileSorter());
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
        ArrayList<String> tFilenames = new ArrayList<>();
        for (int i = 0; i < tFiles.length; i++) {
            //System.out.println(tFiles[i].getAbsoluteFile().toString() + "  " + sdf.format(tFiles[i].lastModified()));
            tFilenames.add(tFiles[i].getAbsoluteFile().toString());
        }
        return tFilenames;
    }

    class FileSorter implements Comparator<File>
    {
        @Override
        public int compare(File f1, File f2) {
            return (int) (f1.lastModified() - f2.lastModified());
        }
    }
}
