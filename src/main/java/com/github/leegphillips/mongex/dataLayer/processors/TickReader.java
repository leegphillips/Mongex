package com.github.leegphillips.mongex.dataLayer.processors;

import com.github.leegphillips.mongex.dataLayer.dao.CurrencyPair;
import com.github.leegphillips.mongex.dataLayer.dao.Tick;
import com.github.leegphillips.mongex.dataLayer.utils.Utils;
import com.github.leegphillips.mongex.dataLayer.utils.WrappedBlockingQueue;

import java.io.*;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static java.util.stream.Collectors.toList;

public class TickReader extends WrappedBlockingQueue<Tick> implements Runnable {
    private static final String CSV_SUFFIX = ".csv";

    private static final int BUFFER_SIZE = 4096;

    private static final File[] FILES = Utils.getFiles();

    private final AtomicInteger filesCompleted = new AtomicInteger(0);
    private final CurrencyPair pair;
    private final List<File> filesForPair;

    public TickReader(CurrencyPair pair, LocalDate base) {
        this(pair, Arrays.stream(FILES)
                .filter(file -> file.getName().contains(pair.getLabel()))
                .filter(file -> !Utils.file2Date(file).isBefore(base))
                .sorted(Comparator.comparing(File::getName))
                .collect(toList()));
    }

    TickReader(CurrencyPair pair, List<File> filesForPair) {
        this.pair = pair;
        this.filesForPair = filesForPair;
    }

    @Override
    public void run() {
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
    }

    public int getFilesCompleted() {
        return filesCompleted.get();
    }
}

