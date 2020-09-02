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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.github.leegphillips.mongex.DocumentFactory.TIMESTAMP_FIELD;
import static org.github.leegphillips.mongex.PropertiesSingleton.*;

public class DataLoader {
    private static Logger log = LoggerFactory.getLogger(DataLoader.class);

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
        int blockSize = Integer.valueOf(properties.getProperty(BLOCK_SIZE_KEY));

        for (File file : new File(properties.getProperty(SOURCE_DIR)).listFiles()) {
            File csvFile = extractor.extractCSV(file);
            MongoCollection<Document> tickCollection = db.getCollection(getPair(csvFile) + "_TICKS");
            if (tickCollection.listIndexes().first() == null) {
                tickCollection.createIndex(Indexes.ascending(TIMESTAMP_FIELD));
            }

            Reader fileReader = new FileReader(csvFile);
            BufferedReader bufferedFileReader = new BufferedReader(fileReader);
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(bufferedFileReader);

//            List<Document> documents = StreamSupport.stream(
//                    Spliterators.spliteratorUnknownSize(records.iterator(), Spliterator.ORDERED), false)
//                        .map(record -> df.create(record))
//                        .filter(document -> document != null)
//                        .collect(Collectors.toList());

            List<Document> documents = new ArrayList<>();
//            records.forEach(record -> documents.add(df.create(record)));

            for (CSVRecord record : records) {
                documents.add(df.create(record));
            }

            bufferedFileReader.close();
            fileReader.close();

            for (int pos = 0; pos < documents.size(); pos += blockSize) {
                int end = pos + blockSize > documents.size() ? documents.size() : pos + blockSize;
                log.info("Adding " + (end - pos) + " records");
                tickCollection.insertMany(documents.subList(pos, end));
            }

            csvFile.delete();
            Files.move(file.toPath(), Paths.get(properties.getProperty(PROCESSED_DIR), file.getName()));
        }
    }

    private String getPair(File csvFile) {
        return csvFile.getName().substring(10, 16);
    }
}
