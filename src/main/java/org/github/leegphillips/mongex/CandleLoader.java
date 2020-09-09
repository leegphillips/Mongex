package org.github.leegphillips.mongex;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class CandleLoader extends AbstractLoader {
    private static final Logger log = LoggerFactory.getLogger(CandleLoader.class);

    private static final SimpleDateFormat FORMATTER = new SimpleDateFormat("dd");
    private static final CandleFactory CANDLE_FACTORY = new CandleFactory();

    public CandleLoader(Properties properties, MongoDatabase db, ZipExtractor extractor, DocumentFactory df) {
        super(properties, db, extractor, df);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties properties = PropertiesSingleton.getInstance();
        MongoDatabase db = DatabaseFactory.create(properties);
        ZipExtractor extractor = new ZipExtractor();
        DocumentFactory df = new DocumentFactory();
        new CandleLoader(properties, db, extractor, df).execute();
    }

    @Override
    protected void processRecords(CSVParser records, MongoCollection<Document> tickCollection) throws IOException {
        List<Document> candles = new ArrayList<>();
        List<CSVRecord> batch = new ArrayList<>();
        String batchDay = null;
        for (CSVRecord record : records) {
            String tickDay = FORMATTER.format(new Date(Long.parseLong(record.get(0).replaceAll("\\s", ""))));
            if (batchDay == null) {
                batchDay = tickDay;
            }

            batch.add(record);
            if (!batchDay.equals(tickDay)) {
                batchDay = tickDay;
                candles.add(CANDLE_FACTORY.create(batch));
                batch = new ArrayList<>();
            }
        }
        candles.add(CANDLE_FACTORY.create(batch));
        log.info("Adding " + candles.size() + " candles");
        tickCollection.insertMany(candles);
    }

    @Override
    protected String getNamespace() {
        return "_CANDLES";
    }
}
