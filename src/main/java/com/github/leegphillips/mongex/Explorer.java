package com.github.leegphillips.mongex;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import static com.mongodb.client.model.Filters.eq;

public class Explorer {
    private static final MongoClient mongoClient = new MongoClient("localhost", 27017);
    private static final MongoDatabase db = mongoClient.getDatabase("Forex");

    public static void main(String[] args) {
//        long start = System.currentTimeMillis();
//        db.listCollectionNames().forEach((Consumer<? super String>) name -> System.out.println(name));
//        System.out.println("Time: " + (System.currentTimeMillis() - start));

        MongoCollection<Document> audjpy = db.getCollection("AUDJPY");
        FindIterable<Document> cursor = audjpy.find().sort(new BasicDBObject("timestamp", 1)).limit(2);
        for (Document document : cursor) {
            System.out.println(document);
        }
        System.out.println();
        for (long pos = 20020819173202000L; pos < 20220819173202000L; pos++) {
            Document timestamp = audjpy.find(eq("timestamp", pos)).first();
            if (timestamp != null) {
                System.out.println(timestamp);
            }
        }
    }
}
