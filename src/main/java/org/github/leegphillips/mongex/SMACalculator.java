package org.github.leegphillips.mongex;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.github.leegphillips.mongex.SortingFactory.EARLIEST;
import static org.github.leegphillips.mongex.SortingFactory.LATEST;

public class SMACalculator {
    private static final Logger LOG = LoggerFactory.getLogger(SMACalculator.class);

    private static final String COLLECTION_NAME = "SMAS";
    private static final int[] COMMON_SMAS = new int[]{10, 20, 50, 100, 200};
    private static final int WINDOW_SIZE = COMMON_SMAS[COMMON_SMAS.length - 1];

    private final MongoDatabase db;

    public SMACalculator(MongoDatabase db) {
        this.db = db;

        // it is essential that this array is sorted
        for (int i = 1, last = COMMON_SMAS[0]; i <= COMMON_SMAS.length - 1; i++) {
            int current = COMMON_SMAS[i];
            if (last > current) {
                throw new IllegalStateException("COMMON_SMAS must be ordered lowest to highest");
            }
            last = current;
        }
    }

    public static void main(String[] args) {
        new SMACalculator(DatabaseFactory.create(PropertiesSingleton.getInstance())).execute();
    }

    // this could be optimised to fetch more than one day at a time
    private void execute() {
        long start = System.currentTimeMillis();
        MongoCollection<Document> candles = db.getCollection(CandleLoader.COLLECTION_NAME);
        MongoCollection<Document> smas = db.getCollection(COLLECTION_NAME);
        smas.createIndex(new Document("date", -1));
        smas.createIndex(new Document("pair", -1));

        //get list of all pairs
        List<String> pairs = new ArrayList<>();
        candles.distinct("pair", String.class).spliterator().forEachRemaining(pairs::add);
        pairs.sort(Comparator.naturalOrder());

        for (String pair : pairs) {
            LOG.info("Processing: " + pair);
            ArrayDeque<BigDecimal> window = new ArrayDeque<>(WINDOW_SIZE);
            LocalDate earliest = LocalDate.parse(candles.find(new Document("pair", pair)).sort(EARLIEST).limit(1).cursor().next().getString("date"));
            LocalDate latest = LocalDate.parse(candles.find(new Document("pair", pair)).sort(LATEST).limit(1).cursor().next().getString("date")).plusDays(1);

            for (LocalDate current = earliest; current.isBefore(latest); current = current.plusDays(1)) {
                Document search = new Document("pair", pair);
                search.append("date", current.toString());

                MongoCursor<Document> cursor = candles.find(search).limit(1).cursor();
                if (!cursor.hasNext()) {
                    // need to handle the case at the start of the dataset where we have consecutive empty days whilst also handling weekends and no trade days
                    continue;
                }
                Document candle = cursor.next();

                // tidy this and correct the rounding - add Candle class, move logic to there
                // also this logic is garbage - generated ticks -> candles -> mids
                window.addFirst(candle.get("high", Decimal128.class).bigDecimalValue()
                        .add(candle.get("low", Decimal128.class).bigDecimalValue())
                        .divide(BigDecimal.valueOf(2), RoundingMode.HALF_EVEN));

                if (window.size() > WINDOW_SIZE) {
                    window.removeLast();
                }

                // need array access
                BigDecimal[] windowSnapshot = window.toArray(new BigDecimal[]{});
                List<Document> result = new ArrayList<>();

                for (int currentSMA : COMMON_SMAS) {
                    if (windowSnapshot.length >= currentSMA) {
                        BigDecimal runningTotal = BigDecimal.ZERO;
                        for (int i = 0; i < currentSMA; i++) {
                            runningTotal = runningTotal.add(windowSnapshot[i]);
                        }
                        BigDecimal smaValue = runningTotal.divide(new BigDecimal(currentSMA), RoundingMode.HALF_EVEN);
                        Document sma = new Document("pair", pair);
                        sma.append("date", current.toString());
                        sma.append("sma", currentSMA);
                        sma.append("value", smaValue);
                        result.add(sma);
                    } else {
                        // assertion that COMMON_SMAS is ordered lowest to highest
                        break;
                    }
                }

                int resultSize = result.size();
                if (resultSize > 0) {
                    smas.insertMany(result);
                    LOG.info("Date: " + current + " Size: " + resultSize);
                }
            }
        }
        LOG.info("Completed in " + (System.currentTimeMillis() - start) + "ms");
    }
}
