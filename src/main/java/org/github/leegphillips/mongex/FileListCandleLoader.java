package org.github.leegphillips.mongex;

import com.mongodb.client.MongoCollection;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.bson.Document;
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
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.stream.Collectors.toList;

public class FileListCandleLoader implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(FileListCandleLoader.class);

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmm");

    private final ZipExtractor extractor;
    private final CandleSpecification candleSpecification;
    private final List<File> allFilesForPair;
    private final MongoCollection<Document> candlesCollection;
    private final AtomicInteger counter;
    private final LocalDateTime chunkPos;
    private final TimeFrame timeFrame;
    private LocalDateTime chunkEnd;
    private Candle pos;

    public FileListCandleLoader(ZipExtractor extractor, CandleSpecification candleSpecification, List<File> allFilesForPair, MongoCollection<Document> candlesCollection, AtomicInteger counter) {
        this.extractor = extractor;
        this.candleSpecification = candleSpecification;
        this.allFilesForPair = allFilesForPair;
        this.candlesCollection = candlesCollection;
        this.counter = counter;
        this.allFilesForPair.sort(File::compareTo);
        this.chunkPos = LocalDateTime.parse(allFilesForPair.get(0).getName().substring(27, 33) + "010000", FORMATTER);
        timeFrame = candleSpecification.getTickSize();
    }

    @Override
    public void run() {
        for (File file : allFilesForPair) {
            try {
                long start = System.currentTimeMillis();
                CurrencyPair pair = new CurrencyPair(file);

                chunkEnd = LocalDateTime.parse(file.getName().substring(27, 33) + "010000", FORMATTER)
                        .plusMonths(1).minusNanos(1);

                File csvFile = extractor.extractCSV(file);
                try (CSVParser records = CSVFormat.DEFAULT.parse(new BufferedReader(new FileReader(csvFile)))) {
                    List<Document> candles = processRecords(records, pair).stream().map(Candle::toDocument).collect(toList());
                    candlesCollection.insertMany(candles);
                    csvFile.delete();
                    LOG.info(file.getName() + " " + candles.size() + " candles " + (System.currentTimeMillis() - start) + "ms Remaining: " + counter.decrementAndGet());
                }
            } catch (IOException e) {
                throw new IllegalStateException("Error processing file: " + file.getName(), e);
            }
        }
    }

    private List<Candle> processRecords(CSVParser records, CurrencyPair pair) {
        List<Candle> candles = new ArrayList<>();
        List<Tick> batch = new ArrayList<>();
        LocalDateTime batchFloor = null;
        LocalDateTime batchCeiling = null;
        TimeFrame tickSize = candleSpecification.getTickSize();

        for (CSVRecord record : records) {
            Tick tick = Tick.create(record);
            LocalDateTime time = tick.getTimestamp();
            if (batchFloor == null) {
                batchFloor = candleSpecification.getFloor(tick.getTimestamp());
                batchCeiling = candleSpecification.getCeiling(batchFloor);
            }

            if (!time.isBefore(batchCeiling)) {
                candles.add(Candle.create(batch, pair, tickSize, batchCeiling));
                batch = new ArrayList<>();
                batchFloor = candleSpecification.getFloor(time);
                batchCeiling = candleSpecification.getCeiling(batchFloor);
            }
            batch.add(tick);
        }
        candles.add(Candle.create(batch, pair, tickSize, batchCeiling));

        padMissingCandles(candles);

        return candles;
    }

    private void padMissingCandles(List<Candle> candles) {
        List<Candle> additions = new ArrayList<>();

        for (Candle candle : candles) {
            if (pos == null) {
                pos = candle;
            } else {
                fill(candle, additions, candle.getTimestamp());
            }
        }
        fill(pos, additions, chunkEnd);
        candles.addAll(additions);
    }

    private void fill(Candle last, List<Candle> additions, LocalDateTime end) {
        LocalDateTime nextSlot = timeFrame.next(pos.getTimestamp());
        while (nextSlot.isBefore(end)) {
            Candle generated = last.generateFrom(nextSlot);
            additions.add(generated);
            nextSlot = timeFrame.next(nextSlot);
            pos = generated;
        }
        pos = last;
    }
}
