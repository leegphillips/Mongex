package com.github.leegphillips.mongex.dataLayer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class CSVExporter {
    public static void main(String[] args) throws IOException {
        TimeFrameMarketStateIterable changes = new TimeFrameMarketStateIterable(TimeFrame.ONE_DAY);
        File file = new File("output.csv");
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.write("Timestamp, EURUSD, USDCHF\n");
        changes.iterator().forEachRemaining(change -> {
            try {
                System.out.println(change);
                writer.write(change.toCSV());
                writer.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
}
