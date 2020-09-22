package org.github.leegphillips.mongex;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Properties;

import static org.github.leegphillips.mongex.DocumentFactory.TIMESTAMP_FIELD;
import static org.github.leegphillips.mongex.PropertiesSingleton.SOURCE_DIR;

public abstract class AbstractLoader {
    private static final Logger log = LoggerFactory.getLogger(AbstractLoader.class);

    protected final Properties properties;
    protected final MongoDatabase db;
    protected final DocumentFactory df;
    private final ZipExtractor extractor;

    public AbstractLoader(Properties properties, MongoDatabase db, ZipExtractor extractor, DocumentFactory df) {
        this.properties = properties;
        this.db = db;
        this.extractor = extractor;
        this.df = df;
    }

    public void execute() throws IOException, ParseException {
        File[] files = new File(properties.getProperty(SOURCE_DIR)).listFiles();
        Arrays.sort(files, (o1, o2) -> o1.getName().compareTo(o2.getName()));

        log.info("Loading " + files.length + " files");
        long start = System.currentTimeMillis();
        int processedCount = 0;

        for (File file : files) {
            log.info((files.length - processedCount) + " files remaining");
            File csvFile = extractor.extractCSV(file);
            MongoCollection<Document> tickCollection = getCollection(csvFile);
            tickCollection.createIndex(Indexes.ascending(TIMESTAMP_FIELD));

            Reader fileReader = new FileReader(csvFile);
            BufferedReader bufferedFileReader = new BufferedReader(fileReader);

            processRecords(CSVFormat.DEFAULT.parse(bufferedFileReader), tickCollection, getPair(csvFile));

            bufferedFileReader.close();
            fileReader.close();

            boolean ignored = csvFile.delete();
            processedCount++;
        }
        log.info(files.length + " loaded in " + (System.currentTimeMillis() - start) + "ms");
    }

    protected MongoCollection<Document> getCollection(File csvFile) {
        return db.getCollection(getPair(csvFile) + getNamespace());
    }

    protected abstract void processRecords(CSVParser records, MongoCollection<Document> tickCollection, String pair) throws IOException, ParseException;

    protected abstract String getNamespace();

    private String getPair(File csvFile) {
        return csvFile.getName().substring(10, 16);
    }
}
