package org.github.leegphillips.mongex;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CurrencyPairTest {
    @Test
    public void happyPath() {
        new CurrencyPair("AUDJPY");
    }

    @SuppressWarnings("ConstantConditions")
    @Test(expected = NullPointerException.class)
    public void cannotCreateWithNull() {
        new CurrencyPair((String) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotCreateWithTooShort() {
        new CurrencyPair("GBP");
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotCreateWithTooLong() {
        new CurrencyPair("CADUSDD");
    }
}
