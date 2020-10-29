package com.github.leegphillips.mongex.dataLayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class TickFileReader extends ArrayBlockingQueue<Tick> {
    private final static Logger LOG = LoggerFactory.getLogger(TickFileReader.class);

    private final static int SIZE = 256;
    private final static String CSV_SUFFIX = ".csv";
    private final static int BUFFER_SIZE = 4096;

    private final File zip;

    public TickFileReader(File zip) {
        super(SIZE);
        this.zip = zip;
    }

    private class Worker implements Runnable {
        @Override
        public void run() {
            try {
                CurrencyPair currencyPair = new CurrencyPair(zip);

                ZipInputStream zis = new ZipInputStream(new FileInputStream(zip));
                BufferedReader br = new BufferedReader(new InputStreamReader(zis), BUFFER_SIZE);

                ZipEntry zipEntry = zis.getNextEntry();
                while (zipEntry != null) {
                    if (zipEntry.getName().endsWith(CSV_SUFFIX)) {
                        break;
                    }
                    zipEntry = zis.getNextEntry();
                }

                String line = br.readLine();
                while (line != null) {
                    String[] values = line.split(",");
                    Tick tick = Tick.create(currencyPair, values[0], values[1], values[2]);
                    LOG.trace(tick.toString());
                    put(tick);
                    line = br.readLine();
                }
                put(Tick.POISON);
                br.close();
            } catch (IOException e) {
                throw new UncheckedIOException(zip.getName(), e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        TickFileReader ticks = new TickFileReader(new File("C:\\Forex\\data\\HISTDATA_COM_ASCII_AUDCHF_T200811.zip"));
        Tick tick = ticks.take();
        while (tick != Tick.POISON) {
            tick = ticks.take();
        }
    }
}
