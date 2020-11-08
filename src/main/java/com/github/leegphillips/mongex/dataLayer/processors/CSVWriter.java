package com.github.leegphillips.mongex.dataLayer.processors;

import com.github.leegphillips.mongex.dataLayer.dao.Classification;
import com.github.leegphillips.mongex.dataLayer.utils.WrappedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicLong;

import static com.github.leegphillips.mongex.dataLayer.dao.Classification.CLOSE;

public class CSVWriter implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(CSVWriter.class);

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    private static final LocalDateTime CUT_OFF = LocalDateTime.parse("2019-11-09 00:00", FORMATTER);

    private final File train;
    private final File eval;
    private final WrappedBlockingQueue<Classification> input;
    private final AtomicLong counter = new AtomicLong(0);
    private final BufferedWriter bwTrain;
    private final BufferedWriter bwEval;
    private String last;

    public CSVWriter(File train, File eval, WrappedBlockingQueue<Classification> input) {
        this.train = train;
        this.eval = eval;
        this.input = input;
        try {
            bwTrain = new BufferedWriter(new FileWriter(train, true));
            bwEval = new BufferedWriter(new FileWriter(eval, true));
        } catch (IOException e) {
            LOG.error(train.getName(), e);
            System.exit(-5);
            throw new UncheckedIOException(train.getName(), e);
        }
    }

    @Override
    public void run() {
        try {
            Classification classification = input.take();
            while (classification != CLOSE) {
                last = classification.getTimestamp().toString();
                if (classification.getTimestamp().isBefore(CUT_OFF)) {
                    bwTrain.write(classification.toCSV());
                    bwTrain.newLine();
                } else {
                    bwEval.write(classification.toCSV());
                    bwEval.newLine();
                }
                counter.incrementAndGet();
                classification = input.take();
            }
            bwTrain.close();
            bwEval.close();
        } catch (IOException e) {
            LOG.error(train.getName() + " " + eval.getName(), e);
            System.exit(-4);
            throw new UncheckedIOException(train.getName(), e);
        }
    }

    public long getLines() {
        return counter.get();
    }

    public String getLast() {
        return last;
    }

    public long getFileSize() {
        try {
            bwTrain.flush();
            bwEval.flush();
            return Files.size(train.toPath()) + Files.size(eval.toPath());
        } catch (IOException e) {
            LOG.error(train.getName() + " " + eval.getName(), e);
            System.exit(-3);
            throw new UncheckedIOException(train.getName() + " " + eval.getName(), e);
        }
    }
}
