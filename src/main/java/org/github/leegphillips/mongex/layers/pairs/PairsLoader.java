package org.github.leegphillips.mongex.layers.pairs;

import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.github.leegphillips.mongex.DatabaseFactory;
import org.github.leegphillips.mongex.PropertiesSingleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.ParseException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.github.leegphillips.mongex.PropertiesSingleton.SOURCE_DIR;

public class PairsLoader {
    private static final Logger log = LoggerFactory.getLogger(PairsLoader.class);

    private final Properties properties;
    private final MongoDatabase db;
    private final PairDocumentFactory pdf;

    public PairsLoader(Properties properties, MongoDatabase db, PairDocumentFactory pdf) {
        this.properties = properties;
        this.db = db;
        this.pdf = pdf;
    }

    public static void main(String[] args) throws ParseException {
        Properties properties = PropertiesSingleton.getInstance();
        MongoDatabase db = DatabaseFactory.create(properties);
        PairDocumentFactory pdf = new PairDocumentFactory();
        new PairsLoader(properties, db, pdf).execute();
    }

    private void execute() {
        File[] files = new File(properties.getProperty(SOURCE_DIR)).listFiles();
        List<Document> pairs = Stream.of(files)
                .map(file -> file.getName().substring(19, 25))
                .distinct()
                .map(pair -> pdf.create(pair))
                .collect(Collectors.toList());
        db.getCollection("Pairs").insertMany(pairs);
    }
}
