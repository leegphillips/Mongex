package org.github.leegphillips.mongex;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CandleLoader extends AbstractLoader {
    private static final Logger log = LoggerFactory.getLogger(CandleLoader.class);

    private final CandleFactory candleFactory = new CandleFactory();

    public CandleLoader(Properties properties, MongoDatabase db, ZipExtractor extractor, DocumentFactory df) {
        super(properties, db, extractor, df);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties properties = PropertiesSingleton.getInstance();
        MongoDatabase db = DatabaseFactory.create(properties);
        ZipExtractor extractor = new ZipExtractor();
        DocumentFactory df = new DocumentFactory();
        new CandleLoader(properties, db, extractor, df).execute();
    }

    @Override
    protected void processRecords(CSVParser records, MongoCollection<Document> tickCollection) throws IOException {
        List<Document> candles = new ArrayList<>();
        List<CSVRecord> batch = new ArrayList<>();
        for (CSVRecord record : records) {
            batch.add(record);
            if (batch.size() == 1000) {
                candles.add(candleFactory.create(batch));
                batch = new ArrayList<>();
            }
        }
        log.info("Adding " + candles.size() + " candles");
        tickCollection.insertMany(candles);
    }
}
