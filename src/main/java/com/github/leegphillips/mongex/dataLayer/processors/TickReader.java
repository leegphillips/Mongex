package com.github.leegphillips.mongex.dataLayer.processors;

import com.github.leegphillips.mongex.dataLayer.CurrencyPair;
import com.github.leegphillips.mongex.dataLayer.Tick;
import com.github.leegphillips.mongex.dataLayer.Utils;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static java.util.stream.Collectors.toList;

public class TickReader extends ArrayBlockingQueue<Tick> implements Runnable {
    private static final String CSV_SUFFIX = ".csv";

    private static final int QUEUE_SIZE = 4096;
    private static final int BUFFER_SIZE = 4096;

    private static final File[] FILES = Utils.getFiles();

    private final AtomicInteger filesCompleted = new AtomicInteger(0);
    private final CurrencyPair pair;

    public TickReader(CurrencyPair pair) {
        super(QUEUE_SIZE);
        this.pair = pair;
    }

    @Override
    public void run() {
        List<File> filesForPair = Arrays.stream(FILES)
                .filter(file -> file.getName().contains(pair.getLabel()))
                .collect(toList());

        try {
            for (File zip : filesForPair) {
                try {
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
                        put(Tick.create(pair, values[0], values[1], values[2]));
                        line = br.readLine();
                    }
                    br.close();
                    filesCompleted.incrementAndGet();
                } catch (IOException e) {
                    throw new UncheckedIOException(zip.getName(), e);
                }
            }
            put(Tick.POISON);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public int getFilesCompleted() {
        return filesCompleted.get();
    }
}

