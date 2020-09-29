package org.github.leegphillips.mongex;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.client.model.Indexes.ascending;
import static java.util.Comparator.comparing;
import static org.github.leegphillips.mongex.PropertiesSingleton.SOURCE_DIR;

public class CandleLoader {
    public static final String COLLECTION_NAME = "CANDLES";

    private static final ExecutorService THREAD_POOL = Executors.newFixedThreadPool(8);

    private static final Logger LOG = LoggerFactory.getLogger(CandleLoader.class);

    private final Properties properties;
    private final MongoDatabase db;
    private final ZipExtractor extractor;
    private final CandleSpecification candleSpecification;

    public CandleLoader(Properties properties, MongoDatabase db, ZipExtractor extractor, CandleSpecification candleSpecification) {
        this.properties = properties;
        this.db = db;
        this.extractor = extractor;
        this.candleSpecification = candleSpecification;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = PropertiesSingleton.getInstance();
        MongoDatabase db = DatabaseFactory.create(properties);
        new CandleLoader(properties, db, new ZipExtractor(), CandleDefinitions.FIVE_MINUTES).execute();
    }

    private void execute() throws ExecutionException, InterruptedException {
        long start = System.currentTimeMillis();

        MongoCollection<Document> candlesCollection = db.getCollection(COLLECTION_NAME);
        candlesCollection.createIndex(ascending(CurrencyPair.ATTR_NAME));
        candlesCollection.createIndex(ascending(TimeFrame.ATTR_NAME));
        candlesCollection.createIndex(ascending(Candle.TIMESTAMP_ATTR_NAME));

        File[] files = new File(properties.getProperty(SOURCE_DIR)).listFiles();
        Arrays.sort(files, comparing(File::getName));
        LOG.info("Loading " + files.length + " files");

        AtomicInteger counter = new AtomicInteger(files.length);
        Collection<Future<?>> futures = new LinkedList<>();
        for (File file : files) {
            Future<?> processing = THREAD_POOL.submit(() -> {
                try {
                    processFile(file, candlesCollection, counter);
                } catch (IOException e) {
                    LOG.error(file.getName(), e);
                }
            });
            futures.add(processing);
        }

        for (Future<?> processing : futures) {
            processing.get();
        }
        LOG.info(files.length + " loaded in " + (System.currentTimeMillis() - start) + "ms");
    }

    private void processFile(File file, MongoCollection<Document> candlesCollection, AtomicInteger counter) throws IOException {
        long start = System.currentTimeMillis();
        CurrencyPair pair = new CurrencyPair(file.getName().substring(19, 25));

        File csvFile = extractor.extractCSV(file);
        Reader fileReader = new FileReader(csvFile);
        BufferedReader bufferedFileReader = new BufferedReader(fileReader);

        List<Document> candles = processRecords(CSVFormat.DEFAULT.parse(bufferedFileReader), pair);
        candlesCollection.insertMany(candles);

        bufferedFileReader.close();
        fileReader.close();

        csvFile.delete();
        LOG.info(file.getName() + " " + candles.size() + " candles " + (System.currentTimeMillis() - start) + "ms Remaining: " + counter.decrementAndGet());
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
