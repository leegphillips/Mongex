package com.github.leegphillips.mongex.dataLayer;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
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
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.stream;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

public class FileListCandleLoader implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(FileListCandleLoader.class);

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmm");

    private final ZipExtractor extractor;
    private final CandleSpecification candleSpecification;
    private final MongoCollection<Document> candlesCollection;
    private final List<File> allFilesForPair;
    private final AtomicInteger counter;
    private final TimeFrame timeFrame;
    private LocalDateTime chunkEnd;
    private Candle pos;

    public static void main(String[] args) {
        ZipExtractor extractor = new ZipExtractor();
        Properties properties = PropertiesSingleton.getInstance();
        MongoDatabase db = DatabaseFactory.create(properties);
        MongoCollection<Document> candles = db.getCollection(CandleLoader.COLLECTION_NAME);
        File[] files = new File(properties.getProperty(PropertiesSingleton.SOURCE_DIR)).listFiles();
        List<File> filesForPair = stream(files).filter(file -> file.getName().contains(args[0])).collect(toList());
        AtomicInteger counter = new AtomicInteger(filesForPair.size());
        new FileListCandleLoader(extractor, CandleDefinitions.FIVE_MINUTES, candles, filesForPair, counter).run();
    }

    public FileListCandleLoader(ZipExtractor extractor, CandleSpecification candleSpecification,
                                MongoCollection<Document> candlesCollection, List<File> allFilesForPair,
                                AtomicInteger counter) {
        this.extractor = extractor;
        this.candleSpecification = candleSpecification;
        this.candlesCollection = candlesCollection;
        this.allFilesForPair = allFilesForPair;
        this.counter = counter;
        this.allFilesForPair.sort(File::compareTo);
        this.timeFrame = candleSpecification.getTickSize();
    }

    @Override
    public void run() {
        for (File file : allFilesForPair) {
            try {
                long start = System.currentTimeMillis();
                CurrencyPair pair = new CurrencyPair(file);

                chunkEnd = LocalDateTime.parse(file.getName().substring(27, 33) + "010000", FORMATTER)
                        .plusMonths(1).minusNanos(1);

                List<Candle> candles = new ArrayList<>();
                File csvFile = extractor.extractCSV(file);
                try (CSVParser records = CSVFormat.DEFAULT.parse(new BufferedReader(new FileReader(csvFile)))) {
                    candles.addAll(padMissingCandles(new CandleBatcher(pair, candleSpecification, new TickPadder(records)).call()));
                }
                csvFile.delete();
                candlesCollection.insertMany(candles.stream().map(Candle::toDocument).collect(toList()));
                LOG.info(file.getName() + " " + candles.size() + " candles " + (System.currentTimeMillis() - start) + "ms Remaining: " + counter.decrementAndGet());
            } catch (IOException e) {
                LOG.error("Error processing file: " + file.getName(), e);
                break;
            }
        }
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

    private class TickPadder implements Iterable<Tick> {

        private final Iterable<CSVRecord> records;

        private TickPadder(Iterable<CSVRecord> records) {
            this.records = records;
        }

        @Override
        public Iterator<Tick> iterator() {

            Iterator<CSVRecord> iterator = records.iterator();

            return new Iterator<Tick>() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public Tick next() {
                    return Tick.create(iterator.next());
                }
            };
        }
    }
}
