package com.github.leegphillips.mongex.dataLayer;

import com.github.leegphillips.mongex.dataLayer.ma.SimpleMovingAverage;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
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
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.stream;
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
    private Tick pos;

    private final List<SimpleMovingAverage> sMAs = stream(new int[]{2, 8, 34, 144, 610, 2584}).mapToObj(SimpleMovingAverage::new).collect(toList());

    public static void main(String[] args) {
        ZipExtractor extractor = new ZipExtractor();
        Properties properties = PropertiesSingleton.getInstance();
        MongoDatabase db = DatabaseFactory.create();
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
                CurrencyPair pair = CurrencyPair.get(file);

                chunkEnd = LocalDateTime.parse(file.getName().substring(27, 33) + "010000", FORMATTER)
                        .plusMonths(1).minusNanos(1);

                List<Candle> candles = new ArrayList<>();
                File csvFile = extractor.extractCSV(file);
                try (CSVParser records = CSVFormat.DEFAULT.parse(new BufferedReader(new FileReader(csvFile)))) {
//                    candles.addAll(new CandleBatcher(pair, candleSpecification, new TickPadder(records), sMAs).call());
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

//    private class TickPadder implements Iterable<Tick> {
//
//        private final Iterable<CSVRecord> records;
//
//        private TickPadder(Iterable<CSVRecord> records) {
//            this.records = records;
//        }
//
//        @Override
//        public Iterator<Tick> iterator() {
//            Iterator<CSVRecord> iterator = records.iterator();
//
//            return new Iterator<Tick>() {
//                Tick next = Tick.create(iterator.next());
//
//                @Override
//                public boolean hasNext() {
//                    return pos == null || timeFrame.next(pos.getTimestamp()).isBefore(chunkEnd);
//                }
//
//                @Override
//                public Tick next() {
//                    if (pos == null) {
//                        pos = next;
//                        next = iterator.hasNext() ? Tick.create(iterator.next()) : null;
//                    } else {
//                        LocalDateTime nextSlot = false ? candleSpecification.getCeiling(pos.getTimestamp()) : timeFrame.next(candleSpecification.getFloor(pos.getTimestamp()));
//                        if (next == null || next.getTimestamp().compareTo(nextSlot) > 0) {
//                            pos = Tick.createInterpolated(pos, nextSlot);
//                        } else {
//                            pos = next;
//                            next = iterator.hasNext() ? Tick.create(iterator.next()) : null;
//                        }
//                    }
//                    return pos;
//                }
//            };
//        }
//    }
}
