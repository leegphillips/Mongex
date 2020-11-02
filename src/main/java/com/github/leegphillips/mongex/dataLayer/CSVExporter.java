package com.github.leegphillips.mongex.dataLayer;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.stream.Collectors.toList;

public class CSVExporter implements Runnable {
    private final static Logger LOG = LoggerFactory.getLogger(CSVExporter.class);

    private final static int QUEUE_SIZE = 256;
    private final static AtomicInteger COUNTER = new AtomicInteger(0);

    private final static ExecutorService SERVICE = Executors.newFixedThreadPool(3);

    private final static Document CLOSE = new Document();

    private final BlockingQueue<Document> pipe = new ArrayBlockingQueue<>(QUEUE_SIZE);

    public static void main(String[] args) {
        new CSVExporter().run();
    }

    @Override
    public void run() {
        SERVICE.execute(new DocumentReader(pipe));
        SERVICE.execute(new CSVWriter(pipe));
        SERVICE.execute(new Monitor(pipe));
    }

    private static class DocumentReader implements Runnable {

        private final BlockingQueue<Document> output;
        private final MongoCursor<Document> states;

        private DocumentReader(BlockingQueue<Document> output) {
            this.output = output;
            Properties properties = PropertiesSingleton.getInstance();
            MongoDatabase db = DatabaseFactory.create(properties);
            MongoCollection<Document> collection = db.getCollection("MAIN STREAM 30s");
            this.states = collection.find().iterator();
        }

        @Override
        public void run() {
            try {
                while (states.hasNext()) {
                    output.put(states.next());
                }
                output.put(CLOSE);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static class CSVWriter implements Runnable {

        private final BlockingQueue<Document> input;

        private CSVWriter(BlockingQueue<Document> input) {
            this.input = input;
        }

        @Override
        public void run() {
            try {
                BufferedWriter bw = new BufferedWriter(new FileWriter("output.csv", true));
                Document doc = input.take();
                while (doc != CLOSE) {
                    for (Map.Entry<String, Object> entry : doc.entrySet()) {
                        String key = entry.getKey();
                        if (!key.equals(Candle.TIMESTAMP_ATTR_NAME) && !key.equals("_id")) {
                            Map<String, Decimal128> map = doc.get(key, Map.class);
                            List<Integer> keys = map.keySet().stream().map(Integer::valueOf).sorted().collect(toList());
                            for (Integer k : keys) {
                                bw.write(map.get(k.toString()).bigDecimalValue().toPlainString());
                                bw.write(", ");
                            }
                        }
                    }
                    bw.newLine();
                    COUNTER.incrementAndGet();

                    doc = input.take();
                }
                bw.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                LOG.error("", e);
            }
            SERVICE.shutdown();
        }
    }

    private static class Monitor implements Runnable {

        private final BlockingQueue<Document> pipe;

        public Monitor(BlockingQueue<Document> pipe) {
            this.pipe = pipe;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    LOG.info("Queue size: " + (QUEUE_SIZE - pipe.remainingCapacity()));
                    LOG.info("Lines written: " + COUNTER.get());
                    Thread.sleep(5000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
