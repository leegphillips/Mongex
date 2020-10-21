package com.github.leegphillips.mongex.dataLayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class TickIterable implements Iterable<Tick> {
    private final static Logger LOG = LoggerFactory.getLogger(TickIterable.class);

    private final static String CSV_SUFFIX = ".csv";
    private final static int BUFFER_SIZE = 4096;

    private final ZipInputStream zis;
    private final BufferedReader br;

    TickIterable(InputStream is) {
        this.zis = new ZipInputStream(is);
        this.br = new BufferedReader(new InputStreamReader(zis), BUFFER_SIZE);
    }

    public TickIterable(File zip) throws IOException {
        this(new FileInputStream(zip));
    }

    @Override
    public Iterator<Tick> iterator() {
        try {
            ZipEntry zipEntry = zis.getNextEntry();
            while (zipEntry != null) {
                if (zipEntry.getName().endsWith(CSV_SUFFIX)) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        return new Iterator<Tick>() {
            String line;
            @Override
            public boolean hasNext() {
                try {
                    line = br.readLine();
                    if (line == null) {
                        br.close();
                        zis.close();
                        return false;
                    }
                    return true;
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public Tick next() {
                String[] values = line.split(",");
                return Tick.create(values[0], values[1], values[2]);
            }
        };
    }
}
