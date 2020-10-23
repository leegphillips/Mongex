package com.github.leegphillips.mongex.dataLayer;

import java.io.File;
import java.util.Properties;
import java.util.stream.Stream;

import static java.util.Arrays.stream;

public class Utils {
    private static final Properties PROPERTIES = PropertiesSingleton.getInstance();

    private static final File[] FILES = new File(PROPERTIES.getProperty(PropertiesSingleton.SOURCE_DIR)).listFiles();

    public static Stream<CurrencyPair> getAllCurrencies() {
        return stream(FILES)
                .map(File::getName)
                .map(name -> name.substring(19, 25))
                .distinct()
                .map(CurrencyPair::new);
    }
}
