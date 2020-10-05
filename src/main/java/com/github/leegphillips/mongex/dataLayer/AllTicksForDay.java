package com.github.leegphillips.mongex.dataLayer;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import static com.mongodb.client.model.Filters.*;

public class AllTicksForDay {
    private static final MongoClient mongoClient = new MongoClient("localhost", 27017);
    private static final MongoDatabase db = mongoClient.getDatabase("Forex");

    public static void main(String[] args) {
        MongoCollection<Document> audjpy = db.getCollection("AUDJPY");
        long start = 20020819000000000L;
        long end = 20020820000000000L;

        FindIterable<Document> cursor = audjpy.find(
                and(
                        gte("timestamp", start),
                        lt("timestamp", end)))
                .sort(new BasicDBObject("timestamp", -1));

        for (Document document : cursor) {
            System.out.println(document);
        }
    }
}
