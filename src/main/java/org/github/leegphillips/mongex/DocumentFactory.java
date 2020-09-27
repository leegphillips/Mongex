package org.github.leegphillips.mongex;

import org.apache.commons.csv.CSVRecord;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentFactory {
    public static final String TIMESTAMP_FIELD = "timestamp";
    public static final String BID_FIELD = "bid";
    public static final String ASK_FIELD = "ask";

    private static final Logger log = LoggerFactory.getLogger(DocumentFactory.class);

    public Document create(CSVRecord record) {
        try {
            // old - sort this out later
            Tick tick = Tick.create(record);
            Document doc = new Document();
            doc.append(TIMESTAMP_FIELD, tick.getTimestamp());
            doc.append(BID_FIELD, tick.getBid());
            doc.append(ASK_FIELD, tick.getAsk());
            return doc;
        } catch (NumberFormatException e) {
            log.error("Error during conversion of " + record, e);
            return null;
        }
    }
}
