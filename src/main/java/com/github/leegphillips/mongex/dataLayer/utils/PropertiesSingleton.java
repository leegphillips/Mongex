package com.github.leegphillips.mongex.dataLayer.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesSingleton {
    public static final String SOURCE_DIR = "dataloader.data.ingest";

    public static final String BATCH_SIZE = "dataloader.behaviour.batchsize";

    public static final String CSV_LOCATION = "csvexporter.data.file";

    private static final Logger log = LoggerFactory.getLogger(PropertiesSingleton.class);

    private static Properties INSTANCE;

    private PropertiesSingleton() {
    }

    public static Properties getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new Properties();
            try (InputStream input = PropertiesSingleton.class.getClassLoader().getResourceAsStream("config.properties")) {
                INSTANCE.load(input);
                log.info("Properties loaded successfully.", INSTANCE);
            } catch (IOException e) {
                log.error("Error during properties load.", e);
            }
        }
        return INSTANCE;
    }
}
