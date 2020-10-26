package com.github.leegphillips.mongex.dataLayer;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@RunWith(MockitoJUnitRunner.class)
public class TickIterableTest {
    @Test
    public void test() throws IOException {
//        String[] rows = new String[]{
//                "20070930 170004000, 0.8832, 0.8822, 0\n",
//                "20070930 170005000, 0.8832, 0.8822, 0\n"};
//        InputStream inputStream = createZip("AUDCAD_T200709", rows);
//        AtomicInteger counter = new AtomicInteger(rows.length);
//        new TickIterable(inputStream).iterator().forEachRemaining(i -> counter.decrementAndGet());
//        assertEquals(0, counter.get());
    }

    private InputStream createZip(String stem, String[] rows) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(byteArrayOutputStream)) {
            zos.putNextEntry(new ZipEntry("DAT_ASCII_" + stem + ".csv"));
            for (String row : rows) {
                zos.write(row.getBytes(), 0, row.length());
            }
            zos.closeEntry();
        }
        return new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
    }
}
