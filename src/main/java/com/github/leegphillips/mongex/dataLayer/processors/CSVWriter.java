package com.github.leegphillips.mongex.dataLayer.processors;

import com.github.leegphillips.mongex.dataLayer.dao.Classification;
import com.github.leegphillips.mongex.dataLayer.utils.WrappedBlockingQueue;

import java.io.*;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicLong;

import static com.github.leegphillips.mongex.dataLayer.dao.Classification.CLOSE;

public class CSVWriter implements Runnable {

    private final File output;
    private final WrappedBlockingQueue<Classification> input;
    private final AtomicLong counter = new AtomicLong(0);
    private final BufferedWriter bw;

    public CSVWriter(File output, WrappedBlockingQueue<Classification> input) {
        this.output = output;
        this.input = input;
        try {
            bw = new BufferedWriter(new FileWriter(output, true));
        } catch (IOException e) {
            throw new UncheckedIOException(output.getName(), e);
        }
    }

    @Override
    public void run() {
        try {
            Classification classification = input.take();
            while (classification != CLOSE) {
                bw.write(classification.toCSV());
                bw.newLine();
                counter.incrementAndGet();
                classification = input.take();
            }
            bw.close();
        } catch (IOException e) {
            throw new UncheckedIOException(output.getName(), e);
        }
    }

    public long getLines() {
        return counter.get();
    }

    public long getFileSize() {
        try {
            bw.flush();
            return Files.size(output.toPath());
        } catch (IOException e) {
            throw new UncheckedIOException(output.getName(), e);
        }
    }
}
