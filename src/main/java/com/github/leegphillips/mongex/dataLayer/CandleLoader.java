package com.github.leegphillips.mongex.dataLayer;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.client.model.Indexes.ascending;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;

public class CandleLoader {
    public static final String COLLECTION_NAME = "CANDLES";

    private static final Logger LOG = LoggerFactory.getLogger(CandleLoader.class);

    private final Properties properties;
    private final MongoCollection<Document> candlesCollection;
    private final ZipExtractor extractor;
    private final CandleSpecification candleSpecification;

    public CandleLoader(Properties properties, MongoDatabase db, ZipExtractor extractor, CandleSpecification candleSpecification) {
        this.properties = properties;
        this.candlesCollection = db.getCollection(COLLECTION_NAME);
        this.extractor = extractor;
        this.candleSpecification = candleSpecification;
    }

    public static void main(String[] args) {
        Properties properties = PropertiesSingleton.getInstance();
        MongoDatabase db = DatabaseFactory.create();
        new CandleLoader(properties, db, new ZipExtractor(), CandleDefinitions.FIVE_MINUTES).execute();
    }

    private void execute() {
        long start = System.currentTimeMillis();

        candlesCollection.createIndex(ascending(CurrencyPair.ATTR_NAME));
        candlesCollection.createIndex(ascending(TimeFrame.ATTR_NAME));
        candlesCollection.createIndex(ascending(Candle.TIMESTAMP_ATTR_NAME));

        File[] files = new File(properties.getProperty(PropertiesSingleton.SOURCE_DIR)).listFiles();

        LOG.info("Loading " + files.length + " files");
        AtomicInteger counter = new AtomicInteger(files.length);

        stream(files)
                .map(CurrencyPair::get)
                .distinct()
                .parallel()
                .map(pair -> stream(files).filter(file -> file.getName().contains(pair.getLabel())).collect(toList()))
                .map(allFilesForPair -> new FileListCandleLoader(extractor, candleSpecification, candlesCollection, allFilesForPair, counter))
                .forEach(FileListCandleLoader::run);

        LOG.info(files.length + " loaded in " + (System.currentTimeMillis() - start) + "ms");
    }
}
