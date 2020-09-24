package org.github.leegphillips.mongex;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import lombok.NonNull;
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

public class SMACalculator {
    private static final Logger LOG = LoggerFactory.getLogger(SMACalculator.class);

    private static final String COLLECTION_NAME = "SMA's";
    private static final int[] COMMON_SMAS = new int[]{10, 20, 50, 100, 200};

    private final MongoDatabase db;
    private final int[] sMAS;

    private final int windowSize;

    public SMACalculator(@NonNull MongoDatabase db, @NonNull CandleSpecification specification, @NonNull int[] sMAS) {
        this.db = db;
        this.sMAS = sMAS;

        if (sMAS.length == 0)
            throw new IllegalArgumentException("SMA's cannot be empty");

        // it is essential that this array is sorted
        for (int i = 1, last = sMAS[0]; i <= sMAS.length - 1; i++) {
            int current = sMAS[i];
            if (last > current)
                throw new IllegalArgumentException("SMA's must be ordered lowest to highest");
            last = current;
        }

        windowSize = sMAS[sMAS.length - 1] * specification.getEventsPerDay();
    }

    public static void main(String[] args) {
        new SMACalculator(DatabaseFactory.create(PropertiesSingleton.getInstance()),
                CandleDefinitions.FIVE_M,
                COMMON_SMAS).execute();
    }

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
            ArrayDeque<BigDecimal> window = new ArrayDeque<>(windowSize);
            List<Document> result = new ArrayList<>();

            MongoCursor<Document> cursor = candles.find(new Document("pair", pair)).sort(new Document("date", 1)).cursor();
            while (cursor.hasNext()) {
                Document candle = cursor.next();

                LocalDate current = LocalDate.parse(candle.getString("date"));

                // tidy this and correct the rounding - add Candle class, move logic to there
                // also this logic is garbage - generated ticks -> candles -> mids
                window.addFirst(candle.get("high", Decimal128.class).bigDecimalValue()
                        .add(candle.get("low", Decimal128.class).bigDecimalValue())
                        .divide(BigDecimal.valueOf(2), RoundingMode.HALF_EVEN));

                if (window.size() > windowSize) {
                    window.removeLast();
                }

                // need array access
                BigDecimal[] windowSnapshot = window.toArray(new BigDecimal[]{});

                for (int currentSMA : sMAS) {
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
            }
            int resultSize = result.size();
            if (resultSize > 0) {
                smas.insertMany(result);
                LOG.info("Pair: " + pair + " Size: " + resultSize);
            }
        }
        LOG.info("Completed in " + (System.currentTimeMillis() - start) + "ms");
    }
}
