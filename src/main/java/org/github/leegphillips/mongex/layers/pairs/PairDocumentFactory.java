package org.github.leegphillips.mongex.layers.pairs;

import org.bson.Document;
import org.github.leegphillips.mongex.objectid.MongoIdFactory;

import java.text.ParseException;

public class PairDocumentFactory {
    private static final String NAMESPACE = "ff";
    private final MongoIdFactory idFactory;

    public PairDocumentFactory() throws ParseException {
        idFactory = new MongoIdFactory();
    }

    public Document create(String pair) {
        Document doc = new Document();
        doc.append("_id", idFactory.getObjectId(NAMESPACE, pair));
        doc.append("pair", pair);
        return doc;
    }
}
