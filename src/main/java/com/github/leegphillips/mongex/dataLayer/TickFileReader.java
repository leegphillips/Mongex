package com.github.leegphillips.mongex.dataLayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class TickFileReader implements Iterable<Tick>, Closeable {
    private final static Logger LOG = LoggerFactory.getLogger(TickFileReader.class);

    private final static String CSV_SUFFIX = ".csv";
    private final static int BUFFER_SIZE = 4096;

    private final ZipInputStream zis;
    private final BufferedReader br;
    private final CurrencyPair currencyPair;

    public TickFileReader(File zip) {
        currencyPair = new CurrencyPair(zip);
        try {
            this.zis = new ZipInputStream(new FileInputStream(zip));
            this.br = new BufferedReader(new InputStreamReader(zis), BUFFER_SIZE);
        } catch (FileNotFoundException e) {
            throw new UncheckedIOException(e);
        }
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
            throw new UncheckedIOException(e);
        }

        return new Iterator<Tick>() {
            @Override
            public boolean hasNext() {
                try {
                    return zis.available() > 0;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public Tick next() {
                try {
                    String line = br.readLine();
                    String[] values = line.split(",");
                    return Tick.create(currencyPair, values[0], values[1], values[2]);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };
    }

    public static void main(String[] args) {
        new TickFileReader(new File("C:\\Forex\\data\\HISTDATA_COM_ASCII_AUDCHF_T200811.zip"))
                .iterator()
                .forEachRemaining(tick -> System.out.println(tick));
    }

    @Override
    public void close() {
        try {
            br.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
