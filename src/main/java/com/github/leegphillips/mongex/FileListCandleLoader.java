package com.github.leegphillips.mongex;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Comparator.comparing;

public class FileListCandleLoader implements Callable<List<Candle>> {
    private static final Logger LOG = LoggerFactory.getLogger(FileListCandleLoader.class);

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmm");

    private final ZipExtractor extractor;
    private final CandleSpecification candleSpecification;
    private final List<File> allFilesForPair;
    private final AtomicInteger counter;
    private final TimeFrame timeFrame;
    private LocalDateTime chunkEnd;
    private Candle pos;

    public FileListCandleLoader(ZipExtractor extractor, CandleSpecification candleSpecification, List<File> allFilesForPair, AtomicInteger counter) {
        this.extractor = extractor;
        this.candleSpecification = candleSpecification;
        this.allFilesForPair = allFilesForPair;
        this.counter = counter;
        this.allFilesForPair.sort(File::compareTo);
        this.timeFrame = candleSpecification.getTickSize();
    }

    @Override
    public List<Candle> call() {
        List<Candle> candles = new ArrayList<>();
        for (File file : allFilesForPair) {
            try {
                long start = System.currentTimeMillis();
                CurrencyPair pair = new CurrencyPair(file);

                chunkEnd = LocalDateTime.parse(file.getName().substring(27, 33) + "010000", FORMATTER)
                        .plusMonths(1).minusNanos(1);

                File csvFile = extractor.extractCSV(file);
                try (CSVParser records = CSVFormat.DEFAULT.parse(new BufferedReader(new FileReader(csvFile)))) {
                    candles.addAll(padMissingCandles(new CandleBatcher(pair, candleSpecification, records).call()));
                }
                csvFile.delete();
                LOG.info(file.getName() + " " + candles.size() + " candles " + (System.currentTimeMillis() - start) + "ms Remaining: " + counter.decrementAndGet());
            } catch (IOException e) {
                LOG.error("Error processing file: " + file.getName(), e);
                break;
            }
        }
        return candles;
    }

    private List<Candle> padMissingCandles(List<Candle> sparseCandles) {
        List<Candle> fullCandles = new ArrayList<>();
        for (Candle candle : sparseCandles) {
            if (pos == null) {
                pos = candle;
            } else {
                fill(candle, fullCandles, candle.getTimestamp());
            }
        }
        fill(pos, fullCandles, chunkEnd);
        fullCandles.addAll(sparseCandles);
        fullCandles.sort(comparing(Candle::getTimestamp));
        return fullCandles;
    }

    private void fill(Candle last, List<Candle> additions, LocalDateTime end) {
        LocalDateTime nextSlot = timeFrame.next(pos.getTimestamp());
        while (nextSlot.isBefore(end)) {
            Candle generated = pos.generateFrom(nextSlot);
            additions.add(generated);
            nextSlot = timeFrame.next(nextSlot);
            pos = generated;
        }
        if (pos.getTimestamp().isBefore(last.getTimestamp())) {
            // the case when we come in with a mid stream tick
            pos = last;
        }
    }
}
