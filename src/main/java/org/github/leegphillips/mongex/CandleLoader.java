package org.github.leegphillips.mongex;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CandleLoader extends AbstractLoader {
    public static final String COLLECTION_NAME = "CANDLES";
    private static final Logger log = LoggerFactory.getLogger(CandleLoader.class);
    private static final CandleFactory CANDLE_FACTORY = new CandleFactory();

    private static final DateTimeFormatter STR2DATE = DateTimeFormatter.ofPattern("yyyyMMdd HHmmssSSS");

    private final CandleSpecification candleSpecification;

    public CandleLoader(Properties properties, MongoDatabase db, ZipExtractor extractor, DocumentFactory df, CandleSpecification candleSpecification) {
        super(properties, db, extractor, df);
        this.candleSpecification = candleSpecification;
    }

    public static void main(String[] args) throws IOException, ParseException {
        Properties properties = PropertiesSingleton.getInstance();
        MongoDatabase db = DatabaseFactory.create(properties);
        ZipExtractor extractor = new ZipExtractor();
        DocumentFactory df = new DocumentFactory();
        new CandleLoader(properties, db, extractor, df, CandleDefinitions.FIVE_M).execute();
    }

    @Override
    protected void processRecords(CSVParser records, MongoCollection<Document> tickCollection, String pair) {
        List<Document> candles = new ArrayList<>();
        List<CSVRecord> batch = new ArrayList<>();
        LocalDateTime batchFloor = null;
        LocalDateTime batchCeiling = null;
        for (CSVRecord record : records) {
            LocalDateTime time = LocalDateTime.parse(record.get(0), STR2DATE);
            if (batchFloor == null) {
                batchFloor = candleSpecification.getFloor(time);
                batchCeiling = candleSpecification.getCeiling(batchFloor);
            }

            if (!time.isBefore(batchCeiling)) {
                candles.add(CANDLE_FACTORY.create(batch, pair, candleSpecification.getTickSize(), batchCeiling));
                batch = new ArrayList<>();
                batchFloor = candleSpecification.getFloor(time);
                batchCeiling = candleSpecification.getCeiling(batchFloor);
            }
            batch.add(record);
        }
        candles.add(CANDLE_FACTORY.create(batch, pair, candleSpecification.getTickSize(), batchCeiling));
        log.info("Adding " + candles.size() + " candles");
        tickCollection.insertMany(candles);
    }

    @Override
    protected String getNamespace() {
        return "_CANDLES";
    }

    @Override
    protected MongoCollection<Document> getCollection(File csvFile) {
        MongoCollection<Document> collection = db.getCollection(COLLECTION_NAME);

        // mongo only creates if one isn't already there - so presumably double calling is fine
        collection.createIndex(Indexes.ascending("pair"));
        collection.createIndex(Indexes.ascending("duration"));
        collection.createIndex(Indexes.ascending("timestamp"));

        return collection;
    }
}
