package org.github.leegphillips.mongex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ZipExtractor {
    private static Logger log = LoggerFactory.getLogger(ZipExtractor.class);

    private final static String CSV_SUFFIX = ".csv";
    private final static int BUFFER_SIZE = 4096;

    public File extractCSV(File zipFile) throws IOException {
        ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile));
        ZipEntry zipEntry = zis.getNextEntry();
        File csvFile = null;
        byte[] buffer = new byte[BUFFER_SIZE];
        while (zipEntry != null) {
            if (zipEntry.getName().endsWith(CSV_SUFFIX)) {
                log.info(zipEntry.getName());
                csvFile = File.createTempFile(zipEntry.getName(), CSV_SUFFIX);
                FileOutputStream fos = new FileOutputStream(csvFile);
                int len;
                while ((len = zis.read(buffer)) > 0) {
                    fos.write(buffer, 0, len);
                }
                fos.close();
                break;
            }
            zipEntry = zis.getNextEntry();
        }
        zis.closeEntry();
        zis.close();

        if (csvFile == null) {
            throw new FileNotFoundException("No CSV in " + zipFile.getName());
        }

        return csvFile;
    }
}
