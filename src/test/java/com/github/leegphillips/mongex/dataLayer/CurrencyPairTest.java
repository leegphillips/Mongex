package com.github.leegphillips.mongex.dataLayer;

import com.github.leegphillips.mongex.dataLayer.dao.CurrencyPair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CurrencyPairTest {
    @Test
    public void happyPath() {
        CurrencyPair.get("AUDJPY");
    }

    @SuppressWarnings("ConstantConditions")
    @Test(expected = NullPointerException.class)
    public void cannotCreateWithNull() {
        CurrencyPair.get((String) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotCreateWithTooShort() {
        CurrencyPair.get("GBP");
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotCreateWithTooLong() {
        CurrencyPair.get("CADUSDD");
    }
}
