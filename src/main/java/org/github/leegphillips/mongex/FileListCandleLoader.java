package org.github.leegphillips.mongex;

import com.mongodb.client.MongoCollection;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

public class FileListCandleLoader implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(FileListCandleLoader.class);

    private final ZipExtractor extractor;
    private final CandleSpecification candleSpecification;
    private final List<File> allFilesForPair;
    private final MongoCollection<Document> candlesCollection;
    private final AtomicInteger counter;

    public FileListCandleLoader(ZipExtractor extractor, CandleSpecification candleSpecification, Stream<File> allFilesForPair, MongoCollection<Document> candlesCollection, AtomicInteger counter) {
        this.extractor = extractor;
        this.candleSpecification = candleSpecification;
        this.allFilesForPair = allFilesForPair.sorted(comparing(File::getName)).collect(toList());
        this.candlesCollection = candlesCollection;
        this.counter = counter;
    }

    @Override
    public void run() {
        for (File file : allFilesForPair) {
            try {
                long start = System.currentTimeMillis();
                CurrencyPair pair = new CurrencyPair(file);

                File csvFile = extractor.extractCSV(file);
                Reader fileReader = new FileReader(csvFile);
                BufferedReader bufferedFileReader = new BufferedReader(fileReader);

                List<Document> candles = processRecords(CSVFormat.DEFAULT.parse(bufferedFileReader), pair);
                candlesCollection.insertMany(candles);

                bufferedFileReader.close();
                fileReader.close();

                csvFile.delete();
                LOG.info(file.getName() + " " + candles.size() + " candles " + (System.currentTimeMillis() - start) + "ms Remaining: " + counter.decrementAndGet());
            } catch (IOException e) {
                throw new IllegalStateException("Error processing file: " + file.getName(), e);
            }
        }
    }

    private List<Document> processRecords(CSVParser records, CurrencyPair pair) {
        List<Document> candles = new ArrayList<>();
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
                candles.add(Candle.create(batch, pair, tickSize, batchCeiling).toDocument());
                batch = new ArrayList<>();
                batchFloor = candleSpecification.getFloor(time);
                batchCeiling = candleSpecification.getCeiling(batchFloor);
            }
            batch.add(Tick.create(record));
        }
        candles.add(Candle.create(batch, pair, tickSize, batchCeiling).toDocument());

        return candles;
    }
}
