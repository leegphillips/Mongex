package com.github.leegphillips.mongex.dataLayer;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class FileListCandleLoaderTest {

    @Mock
    MongoCollection<Document> candlesCollection;

    @Test
    public void happyPathSingleFile() throws IOException {
        File zip1 = createZip("AUDCAD_T200709", "20070930 233504000, 0.8832, 0.8822, 0");

        new FileListCandleLoader(new ZipExtractor(), CandleDefinitions.FIVE_MINUTES,
                candlesCollection, Arrays.asList(zip1), new AtomicInteger(1)).run();

        checkCalls(1, 4);

        zip1.delete();
    }

    @Test
    public void happyPath() throws IOException {
        File zip1 = createZip("AUDCAD_T200709", "20070930 170004000, 0.8832, 0.8822, 0");
//        File zip1 = createZip("AUDCAD_T200709", "20070930 234504000, 0.8832, 0.8822, 0");
        File zip2 = createZip("AUDCAD_T200710", "20071007 170005000, 0.8823, 0.8819, 0");

        new FileListCandleLoader(new ZipExtractor(), CandleDefinitions.FIVE_MINUTES,
                candlesCollection, Arrays.asList(zip1, zip2), new AtomicInteger(1)).run();

        checkCalls(2, 9011);

        zip1.delete();
        zip2.delete();
    }

    private void checkCalls(int callCount, int expectedCandles) {
        ArgumentCaptor<List<Document>> argument = ArgumentCaptor.forClass(List.class);
        verify(candlesCollection, times(callCount)).insertMany(argument.capture());

        Candle prev = null;
        int count = 0;
        for (List<Document> documents : argument.getAllValues()) {
            for (Document document : documents) {
                Candle current = Candle.create(document);
                if (prev != null) {
                    assertTrue(current.getTimestamp().isEqual(
                            CandleDefinitions.FIVE_MINUTES.getTickSize().next(prev.getTimestamp())));
                }
                prev = current;
                count++;
            }
        }
        assertEquals(expectedCandles, count);
    }

    private File createZip(String stem, String row) throws IOException {
        File zip = File.createTempFile("HISTDATA_COM_ASCII_" + stem, ".zip");
        try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zip))) {
            zos.putNextEntry(new ZipEntry("DAT_ASCII_" + stem + ".csv"));
            zos.write(row.getBytes(), 0, row.length());
            zos.closeEntry();
        }
        return zip;
    }
}
