package cwms.cda.api;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RangeParserTest {

    @Test
    void testResume() {
        List<long[]> ranges = RangeParser.parse("bytes=100-");
        assertNotNull(ranges);
        assertEquals(1, ranges.size());
        assertArrayEquals(new long[]{100L, -1L}, ranges.get(0));
    }

    @Test
    void testFirstK() {
        List<long[]> ranges = RangeParser.parse("bytes=0-1000");
        assertNotNull(ranges);
        assertEquals(1, ranges.size());
        assertArrayEquals(new long[]{0L, 1000L}, ranges.get(0));
    }

    @Test
    void testFirstOpen() {
        List<long[]> ranges = RangeParser.parse("bytes=0-");
        assertNotNull(ranges);
        assertEquals(1, ranges.size());
        assertArrayEquals(new long[]{0L, -1L}, ranges.get(0));
    }

    @Test
    void testSuffixOpen() {
        List<long[]> ranges = RangeParser.parse("bytes=-50");
        assertNotNull(ranges);
        assertEquals(1, ranges.size());
        assertArrayEquals(new long[]{-1L, 50L}, ranges.get(0));
    }


    @Test
    void testTwoPart() {
        List<long[]> ranges = RangeParser.parse("bytes=0-10,99-100");
        assertNotNull(ranges);
        assertEquals(2, ranges.size());
        assertArrayEquals(new long[]{0L, 10L}, ranges.get(0));
        assertArrayEquals(new long[]{99L, 100L}, ranges.get(1));
    }


    @Test
    void testMultiParse() {
        List<long[]> ranges = RangeParser.parse("bytes=0-99,200-299,-50");
        assertNotNull(ranges);
        assertEquals(3, ranges.size());
        assertArrayEquals(new long[]{0L, 99L}, ranges.get(0));
        assertArrayEquals(new long[]{200L, 299L}, ranges.get(1));
        assertArrayEquals(new long[]{-1L, 50L}, ranges.get(2));
    }


    @Test
    void testTwoWeird() {
        List<long[]> ranges = RangeParser.parse("bytes=0-0,-1");
        assertNotNull(ranges);
        assertEquals(2, ranges.size());
        assertArrayEquals(new long[]{0L, 0L}, ranges.get(0));
        assertArrayEquals(new long[]{-1L, 1L}, ranges.get(1));
    }

    @Test
    void testNotBytes() {
        assertThrows(IllegalArgumentException.class, () -> RangeParser.parse("bits=0-10"));
    }


    @Test
    void testSuffixDoubleNeg() {
        assertThrows(IllegalArgumentException.class, () -> RangeParser.parse("bytes=--64"));
    }


    @Test
    void testSuffixClosed() {
        assertThrows(IllegalArgumentException.class, () ->
                RangeParser.parse("bytes=-50-100"));
    }


    @Test
    void testSuffixDoubleClosed() {
        assertThrows(IllegalArgumentException.class, () -> RangeParser.parse("bytes=-50--100"));
    }

    @Test
    void testInterpret(){

        assertArrayEquals(new long[]{0L, 10L}, RangeParser.interpret(new long[]{0L, 10L}, 100));
        assertArrayEquals(new long[]{0L, 0L}, RangeParser.interpret(new long[]{0L, 0L}, 100));
        assertArrayEquals(new long[]{8L, 12L}, RangeParser.interpret(new long[]{8L, 12L}, 100));
        assertArrayEquals(new long[]{8L, 99L}, RangeParser.interpret(new long[]{8L, 100L}, 100));
        assertArrayEquals(new long[]{8L, 99L}, RangeParser.interpret(new long[]{8L, 200L}, 100));

        // typical resume bytes=10-
        assertArrayEquals(new long[]{10L, 99L}, RangeParser.interpret(new long[]{10L, -1L}, 100));

        // bytes=0-0
        assertArrayEquals(new long[]{0L, 0L}, RangeParser.interpret(new long[]{0L, 0L}, 100));
        // bytes=-1
        assertArrayEquals(new long[]{99L, 99L}, RangeParser.interpret(new long[]{-1L, 1L}, 100));

        // bytes=-50
        assertArrayEquals(new long[]{50L, 99L}, RangeParser.interpret(new long[]{-1L, 50L}, 100));

    }

    @Test
    void testInvalidInterp() {
        // They requested 100-200 but our file is 100 long (only 0-99).
        assertThrows(IllegalArgumentException.class, () -> RangeParser.interpret(new long[]{100L, 200L}, 100));
        assertThrows(IllegalArgumentException.class, () -> RangeParser.interpret(new long[]{200L, 100L}, 100));
        assertThrows(IllegalArgumentException.class, () -> RangeParser.interpret(new long[]{100L, 100L}, 100));

    }

}
