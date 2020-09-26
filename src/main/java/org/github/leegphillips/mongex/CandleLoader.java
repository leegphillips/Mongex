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
    private static final Logger LOG = LoggerFactory.getLogger(CandleLoader.class);

    public static final String COLLECTION_NAME = "CANDLES";

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
        new CandleLoader(properties, db, extractor, df, CandleDefinitions.FIVE_MINUTES).execute();
    }

    @Override
    protected void processRecords(CSVParser records, MongoCollection<Document> tickCollection, CurrencyPair pair) {
        List<Document> candles = new ArrayList<>();
        List<Tick> batch = new ArrayList<>();
        LocalDateTime batchFloor = null;
        LocalDateTime batchCeiling = null;
        CandleSize tickSize = candleSpecification.getTickSize();

        for (CSVRecord record : records) {
            LocalDateTime time = LocalDateTime.parse(record.get(0), STR2DATE);
            if (batchFloor == null) {
                batchFloor = candleSpecification.getFloor(time);
                batchCeiling = candleSpecification.getCeiling(batchFloor);
            }

            if (!time.isBefore(batchCeiling)) {
                candles.add(Candle.create(batch, pair, tickSize, batchCeiling).toDocument());
                batch = new ArrayList<>();
                batchFloor = candleSpecification.getFloor(time);
                batchCeiling = candleSpecification.getCeiling(batchFloor);
            }
            batch.add(new Tick(record));
        }
        candles.add(Candle.create(batch, pair, tickSize, batchCeiling).toDocument());
        LOG.info("Adding " + candles.size() + " candles");
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
