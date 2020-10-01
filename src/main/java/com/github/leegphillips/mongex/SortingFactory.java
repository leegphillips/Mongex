package com.github.leegphillips.mongex;

import org.bson.Document;

public class SortingFactory {
    public final static Document EARLIEST = new Document("date", 1);
    public final static Document LATEST = new Document("date", -1);
}
