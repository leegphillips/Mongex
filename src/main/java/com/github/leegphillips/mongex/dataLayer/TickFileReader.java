package com.github.leegphillips.mongex.dataLayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class TickFileReader implements Closeable {
    private final static Logger LOG = LoggerFactory.getLogger(TickFileReader.class);

    private final static String CSV_SUFFIX = ".csv";
    private final static int BUFFER_SIZE = 4096;

    private final File zip;
    private final BufferedReader br;
    private final CurrencyPair currencyPair;

    public TickFileReader(File zip) {
        this.zip = zip;

        try {
            currencyPair = new CurrencyPair(zip);

            ZipInputStream zis = new ZipInputStream(new FileInputStream(zip));
            br = new BufferedReader(new InputStreamReader(zis), BUFFER_SIZE);

            ZipEntry zipEntry = zis.getNextEntry();
            while (zipEntry != null) {
                if (zipEntry.getName().endsWith(CSV_SUFFIX)) {
                    break;
                }
                zipEntry = zis.getNextEntry();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(zip.getName(), e);
        }
    }

    public Tick readTick() {
        try {
            String line = br.readLine();

            if (line == null)
                return null;

            String[] values = line.split(",");
            Tick tick = Tick.create(currencyPair, values[0], values[1], values[2]);
            LOG.trace(tick.toString());
            return tick;
        } catch (IOException e) {
            throw new UncheckedIOException(zip.getName(), e);
        }
    }

    @Override
    public void close() {
        try {
            br.close();
        } catch (IOException e) {
            throw new UncheckedIOException(zip.getName(), e);
        }
    }

    public static void main(String[] args) {
        TickFileReader ticks = new TickFileReader(new File("C:\\Forex\\data\\HISTDATA_COM_ASCII_AUDCHF_T200811.zip"));
        Tick tick = ticks.readTick();
        while (tick != null) {
            tick = ticks.readTick();
        }
        ticks.close();
    }
}
