package com.github.leegphillips.mongex.dataLayer.utils;

import com.github.leegphillips.mongex.dataLayer.dao.CurrencyPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.stream.Stream;

import static java.util.Arrays.stream;

public class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    private static final DateTimeFormatter FILE2DATE = DateTimeFormatter.ofPattern("yyyyMMdd");

    private static final Properties PROPERTIES = PropertiesSingleton.getInstance();

    private static final File[] FILES = new File(PROPERTIES.getProperty(PropertiesSingleton.SOURCE_DIR)).listFiles();

    public static Stream<CurrencyPair> getAllCurrencies() {
        LOG.info(PropertiesSingleton.SOURCE_DIR);
        LOG.info(PROPERTIES.getProperty(PropertiesSingleton.SOURCE_DIR));
        LOG.info("" + new File(PROPERTIES.getProperty(PropertiesSingleton.SOURCE_DIR)).exists());
        return stream(FILES)
                .map(File::getName)
                .map(name -> name.substring(19, 25))
                .distinct()
                .map(CurrencyPair::get);
    }

    public static File[] getFiles() {
        return FILES;
    }

    public static LocalDate file2Date(File file) {
        String name = file.getName();
        LocalDate date = LocalDate.parse(name.substring(27, 33) + "01", FILE2DATE);
        return date;
    }
}
