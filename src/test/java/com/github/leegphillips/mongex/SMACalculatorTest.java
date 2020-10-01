package com.github.leegphillips.mongex;

import com.mongodb.client.MongoDatabase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SMACalculatorTest {

    @Mock
    MongoDatabase db;

    @Mock
    CandleSpecification specification;

    @Test
    public void testInstantiation() {
        new SMACalculator(db, specification, new int[]{1, 2});
    }

    @Test(expected = NullPointerException.class)
    public void testNullDBThrows() {
        new SMACalculator(null, specification, new int[]{});
    }

    @Test(expected = NullPointerException.class)
    public void testNullSpecificationThrows() {
        new SMACalculator(db, null, new int[]{1, 2});
    }

    @Test(expected = NullPointerException.class)
    public void testNullSMAListThrows() {
        new SMACalculator(db, specification, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSMAListCannotBeEmpty() {
        new SMACalculator(db, specification, new int[]{});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSMAsCannotBeUnordered() {
        new SMACalculator(db, specification, new int[]{2, 1});
    }
}
