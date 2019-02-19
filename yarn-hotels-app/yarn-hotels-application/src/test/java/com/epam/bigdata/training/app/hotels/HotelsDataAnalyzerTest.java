package com.epam.bigdata.training.app.hotels;

import org.junit.Assert;
import org.junit.Test;

public class HotelsDataAnalyzerTest {

    @Test
    public void extractHeaders() {
        // when & then
        Assert.assertArrayEquals(
                new String[] {"a", "b", "", "", "c"},
                HotelsDataAnalyzer.extractHeaders("a,b,,,c")
        );
    }

    @Test
    public void findIndex() {
        // given
        String[] input = new String[] {"a", "b", "", "", "c"};

        // when & then
        Assert.assertEquals(0, HotelsDataAnalyzer.findIndex(input, "a"));
        Assert.assertEquals(1, HotelsDataAnalyzer.findIndex(input, "b"));
        Assert.assertEquals(4, HotelsDataAnalyzer.findIndex(input, "c"));
        Assert.assertEquals(-1, HotelsDataAnalyzer.findIndex(input, "d"));
    }

    @Test
    public void buildHotelCompositeKey() {
        // when & then
        Assert.assertEquals("country@market", HotelsDataAnalyzer.buildHotelCompositeKey("country", "market"));
    }

    @Test
    public void getSafely() {
        // given
        String[] input = new String[] {"a", "b", "", "", "c"};

        // when & then
        Assert.assertEquals("a", HotelsDataAnalyzer.getSafely(input, 0));
        Assert.assertNull(HotelsDataAnalyzer.getSafely(input, 10));
    }

    @Test
    public void anyNull() {
         // when & then
        Assert.assertTrue(HotelsDataAnalyzer.anyNull("a", null, "b"));
        Assert.assertFalse(HotelsDataAnalyzer.anyNull());
        Assert.assertFalse(HotelsDataAnalyzer.anyNull("a", "c", "b"));
    }
}
