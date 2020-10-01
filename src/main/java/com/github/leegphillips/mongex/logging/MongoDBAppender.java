package com.github.leegphillips.mongex.logging;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import com.github.leegphillips.mongex.DatabaseFactory;
import com.github.leegphillips.mongex.PropertiesSingleton;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Date;
import java.util.Map;

// TODO move to ELK later
public class MongoDBAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {

    private final MongoCollection<Document> logs;

    public MongoDBAppender() {
        MongoDatabase db = DatabaseFactory.create(PropertiesSingleton.getInstance());
        logs = db.getCollection("logs");
    }

    @Override
    protected void append(ILoggingEvent eventObject) {
        Document logEntry = new Document();
        logEntry.append("message", eventObject.getFormattedMessage());
        logEntry.append("logger", eventObject.getLoggerName());
        logEntry.append("thread", eventObject.getThreadName());
        logEntry.append("timestamp", new Date(eventObject.getTimeStamp()));
        logEntry.append("level", eventObject.getLevel().toString());
        Map<String, String> data = eventObject.getMDCPropertyMap();
        for (String key : data.keySet()) {
            logEntry.append(key, data.get(key));
        }
        logs.insertOne(logEntry);
    }
}