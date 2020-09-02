package org.github.leegphillips.mongex;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.github.leegphillips.mongex.DocumentFactory.TIMESTAMP_FIELD;
import static org.github.leegphillips.mongex.PropertiesSingleton.*;

public class DataLoader {
    private static final Logger log = LoggerFactory.getLogger(DataLoader.class);

    private final Properties properties;
    private final MongoDatabase db;
    private final ZipExtractor extractor;
    private final DocumentFactory df;

    public DataLoader(Properties properties, MongoDatabase db, ZipExtractor extractor, DocumentFactory df) {
        this.properties = properties;
        this.db = db;
        this.extractor = extractor;
        this.df = df;
    }

    public static void main(String[] args) throws IOException {
        Properties properties = PropertiesSingleton.getInstance();
        MongoDatabase db = DatabaseFactory.create(properties);
        ZipExtractor extractor = new ZipExtractor();
        DocumentFactory df = new DocumentFactory();
        new DataLoader(properties, db, extractor, df).execute();
    }

    public void execute() throws IOException {
        for (File file : new File(properties.getProperty(SOURCE_DIR)).listFiles()) {
            File csvFile = extractor.extractCSV(file);
            MongoCollection<Document> tickCollection = db.getCollection(getPair(csvFile) + "_TICKS");
            if (tickCollection.listIndexes().first() == null) {
                tickCollection.createIndex(Indexes.ascending(TIMESTAMP_FIELD));
            }

            Reader fileReader = new FileReader(csvFile);
            BufferedReader bufferedFileReader = new BufferedReader(fileReader);

            List<Document> documents = new ArrayList<>();
            int batchSize = Integer.valueOf(properties.getProperty(BATCH_SIZE));
            for (CSVRecord record : CSVFormat.DEFAULT.parse(bufferedFileReader)) {
                documents.add(df.create(record));
                if (documents.size() == batchSize) {
                    log.info("Adding " + batchSize + " records");
                    tickCollection.insertMany(documents);
                    documents = new ArrayList<>();
                }
            }
            log.info("Adding " + documents.size() + " records");
            tickCollection.insertMany(documents);

            bufferedFileReader.close();
            fileReader.close();

            csvFile.delete();
            if (Boolean.valueOf(properties.getProperty(MOVE_FILES))) {
                Files.move(file.toPath(), Paths.get(properties.getProperty(PROCESSED_DIR), file.getName()));
            }
        }
    }

    private String getPair(File csvFile) {
        return csvFile.getName().substring(10, 16);
    }
}
