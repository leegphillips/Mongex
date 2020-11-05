package com.github.leegphillips.mongex.dataLayer.dao;

import com.github.leegphillips.mongex.dataLayer.CandleLoader;
import com.github.leegphillips.mongex.dataLayer.CurrencyPair;
import com.github.leegphillips.mongex.dataLayer.DatabaseFactory;
import com.github.leegphillips.mongex.dataLayer.PropertiesSingleton;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class CurrencyPairDAO {
    private final MongoCollection<Document> candles;

    public CurrencyPairDAO() {
        this(DatabaseFactory.create().getCollection(CandleLoader.COLLECTION_NAME));
    }

    CurrencyPairDAO(MongoCollection<Document> candles) {
        this.candles = candles;
    }

    public List<CurrencyPair> getAll() {
        List<CurrencyPair> pairs = new ArrayList<>();
        candles.distinct(CurrencyPair.ATTR_NAME, String.class)
                .map(CurrencyPair::get)
                .iterator()
                .forEachRemaining(pairs::add);
        return pairs;
    }
}
