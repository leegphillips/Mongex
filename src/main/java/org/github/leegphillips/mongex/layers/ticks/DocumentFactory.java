package org.github.leegphillips.mongex.layers.ticks;

import org.apache.commons.csv.CSVRecord;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentFactory {
    public static final String TIMESTAMP_FIELD = "timestamp";
    public static final String BID_FIELD = "bid";
    public static final String ASK_FIELD = "ask";

    private static final Logger log = LoggerFactory.getLogger(DocumentFactory.class);

    public Document create(CSVRecord record) {
        try {
            Document doc = new Document();
            String timestamp = record.get(0).replaceAll("\\s", "");
            doc.append(TIMESTAMP_FIELD, Long.valueOf(timestamp));
            doc.append(BID_FIELD, Decimal128.parse(record.get(2).trim()));
            doc.append(ASK_FIELD, Decimal128.parse(record.get(1).trim()));
            return doc;
        } catch (NumberFormatException e) {
            log.error("Error during conversion of " + record, e);
            return null;
        }
    }
}
