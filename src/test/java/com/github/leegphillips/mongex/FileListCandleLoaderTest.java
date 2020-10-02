package com.github.leegphillips.mongex;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class FileListCandleLoaderTest {
    @Test
    public void happyPath() throws IOException {
        File zip1 = createZip("AUDCAD_T200709", "20070930 170004000, 0.8832, 0.8822, 0");
        File zip2 = createZip("AUDCAD_T200710", "20071007 170005000, 0.8823, 0.8819, 0");

        List<Candle> candles = new FileListCandleLoader(new ZipExtractor(), CandleDefinitions.FIVE_MINUTES,
                Arrays.asList(zip1, zip2), new AtomicInteger(1)).call();

        Candle prev = null;
        for (Candle current : candles) {
            if (prev != null) {
                System.out.println(current);
                assertTrue(current.getTimestamp().isEqual(
                        CandleDefinitions.FIVE_MINUTES.getTickSize().next(prev.getTimestamp())));
            }
            prev = current;
        }

        zip1.delete();
        zip2.delete();
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
