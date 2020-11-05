package com.github.leegphillips.mongex.dataLayer;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class DatabaseFactory {
    private final static String DB_NAME = "Mongex";

    private final static Pattern INDIVIDUAL = Pattern.compile("^[A-Z]{6}.*$");

    private static MongoDatabase database;

    public static MongoDatabase create() {
        if (database == null) {
            Properties properties = PropertiesSingleton.getInstance();
            MongoClient mongoClient = new MongoClient(
                    properties.getProperty("dataloader.mongo.host"),
                    Integer.parseInt(properties.getProperty("dataloader.mongo.port")));
            database = mongoClient.getDatabase(DB_NAME);
        }
        return database;
    }

    public static MongoCollection<Document> getStream(CurrencyPair pair, TimeFrame tf) {
        create();

        MongoCollection<Document> stream = database.getCollection(pair.getLabel() + " " + tf.getLabel());
        stream.createIndex(new Document(Candle.TIMESTAMP_ATTR_NAME, 1));

        return stream;
    }

    public static Map<String, MongoCollection<Document>> getIndividualStreams(TimeFrame tf) {
        create();

        List<String> names = database.listCollectionNames()
                .into(new ArrayList<>())
                .parallelStream()
                .filter(name -> name.contains(tf.getLabel()))
                .filter(name -> INDIVIDUAL.matcher(name).find())
                .collect(toList());

        Map<String, MongoCollection<Document>> result = names.stream()
                .collect(toMap(Function.identity(), database::getCollection))
                .entrySet().stream()
                    .collect(toMap(entry -> entry.getKey().substring(0, 6), Map.Entry::getValue));

        return result;
    }

    public static MongoCollection<Document> getMainStream(TimeFrame tf) {
        create();

        MongoCollection<Document> stream = database.getCollection(tf.getLabel());
        stream.createIndex(new Document(Candle.TIMESTAMP_ATTR_NAME, 1));

        return stream;
    }
}
