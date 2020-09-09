package org.github.leegphillips.mongex;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.github.leegphillips.mongex.PropertiesSingleton.BATCH_SIZE;

public class TickLoader extends AbstractLoader {
    private static final Logger log = LoggerFactory.getLogger(TickLoader.class);

    public TickLoader(Properties properties, MongoDatabase db, ZipExtractor extractor, DocumentFactory df) {
        super(properties, db, extractor, df);
    }

    public static void main(String[] args) throws IOException, InterruptedException, ParseException {
        Properties properties = PropertiesSingleton.getInstance();
        MongoDatabase db = DatabaseFactory.create(properties);
        ZipExtractor extractor = new ZipExtractor();
        DocumentFactory df = new DocumentFactory();
        new TickLoader(properties, db, extractor, df).execute();
    }

    @Override
    protected void processRecords(CSVParser records, MongoCollection<Document> tickCollection) {
        List<Document> documents = new ArrayList<>();
        int batchSize = Integer.parseInt(properties.getProperty(BATCH_SIZE));
        for (CSVRecord record : records) {
            documents.add(df.create(record));
            if (documents.size() == batchSize) {
                log.info("Adding " + batchSize + " records");
                tickCollection.insertMany(documents);
                documents = new ArrayList<>();
            }
        }
        log.info("Adding " + documents.size() + " records");
        tickCollection.insertMany(documents);
    }

    @Override
    protected String getNamespace() {
        return "_TICKS";
    }
}
