package org.github.leegphillips.mongex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesSingleton {
    public static final String PROCESSED_DIR = "dataloader.data.processed";
    public static final String SOURCE_DIR = "dataloader.data.ingest";

    public static final String MOVE_FILES = "dataloader.behaviour.movefiles";
    public static final String BATCH_SIZE = "dataloader.behaviour.batchsize";
    public static final String PAUSE_INTERVAL = "dataloader.behaviour.pause";

    private static final Logger log = LoggerFactory.getLogger(PropertiesSingleton.class);

    private static Properties INSTANCE;

    private PropertiesSingleton() {
    }

    public static Properties getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new Properties();
            try (InputStream input = AbstractLoader.class.getClassLoader().getResourceAsStream("config.properties")) {
                INSTANCE.load(input);
                log.info("Properties loaded successfully.", INSTANCE);
            } catch (IOException e) {
                log.error("Error during properties load.", e);
            }
        }
        return INSTANCE;
    }
}
