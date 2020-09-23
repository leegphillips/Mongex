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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class CandleLoader extends AbstractLoader {
    private static final Logger log = LoggerFactory.getLogger(CandleLoader.class);

    public static final String COLLECTION_NAME = "CANDLES 1D";

    private static final CandleFactory CANDLE_FACTORY = new CandleFactory();
    private static final SimpleDateFormat DATE_TO_DAY = new SimpleDateFormat("dd");
    private static final SimpleDateFormat STRING_TO_DATE = new SimpleDateFormat("yyyyMMdd HHmmssSSS");

    public CandleLoader(Properties properties, MongoDatabase db, ZipExtractor extractor, DocumentFactory df) {
        super(properties, db, extractor, df);
    }

    public static void main(String[] args) throws IOException, ParseException {
        Properties properties = PropertiesSingleton.getInstance();
        MongoDatabase db = DatabaseFactory.create(properties);
        ZipExtractor extractor = new ZipExtractor();
        DocumentFactory df = new DocumentFactory();
        new CandleLoader(properties, db, extractor, df).execute();
    }

    @Override
    protected void processRecords(CSVParser records, MongoCollection<Document> tickCollection, String pair) throws ParseException {
        List<Document> candles = new ArrayList<>();
        List<CSVRecord> batch = new ArrayList<>();
        String batchDay = null;
        for (CSVRecord record : records) {
            String timestamp = record.get(0);
            Date tickDate = STRING_TO_DATE.parse(timestamp);
            String tickDay = DATE_TO_DAY.format(tickDate);
            if (batchDay == null) {
                batchDay = tickDay;
            }

            if (!batchDay.equals(tickDay)) {
                batchDay = tickDay;
                candles.add(CANDLE_FACTORY.create(batch, pair));
                batch = new ArrayList<>();
            }
            batch.add(record);
        }
        candles.add(CANDLE_FACTORY.create(batch, pair));
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

        return collection;
    }
}
