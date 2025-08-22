package helpers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import cwms.cda.formatters.json.JsonV2;
import java.time.Instant;
import java.time.ZonedDateTime;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class SameEpochMillis extends TypeSafeMatcher<Long> {
    private final long expected;

    public SameEpochMillis(long expected) {
        this.expected = expected;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(" must parse to the same Instant as " + expected + " (" + Instant.ofEpochMilli(expected) + ")");
    }

    @Override
    protected boolean matchesSafely(Long inputMilli) {
        return expected == inputMilli;
    }

    public static Instant jacksonParse(String inputStr) throws JsonProcessingException {
        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        ObjectReader objectReader = objectMapper.readerFor(Holder.class);
        Holder obj = objectReader.readValue("{\"instant\":\"" + inputStr + "\"}");
        return obj.instant;
    }

    @Factory
    public static Matcher<Long> sameEpochMillis(String expectedStr) {
        Instant instant;

        try {
            instant = jacksonParse(expectedStr);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return sameEpochMillis(instant);
    }

    @Factory
    public static Matcher<Long> sameEpochMillis(Instant expectedInstant) {
        return sameEpochMillis(expectedInstant.toEpochMilli());
    }

    @Factory
    public static Matcher<Long> sameEpochMillis(long expected) {
        return new SameEpochMillis(expected);
    }

    @Factory
    public static Matcher<Long> sameEpochMillis(ZonedDateTime expectedZdt) {
        return sameEpochMillis(expectedZdt.toInstant());
    }

    private static class Holder {
        public Holder() {
        }

        public Instant instant;
    }

    // junit style
    public static void assertSameEpoch(long expected, Long actual, String message) {
        assertEquals(expected, actual, message);
    }

    public static void assertSameEpoch(long expected, String actual, String message) throws JsonProcessingException {
        Instant actualInstant = jacksonParse(actual);
        assertSameEpoch(expected, actualInstant, message);
    }

    public static void assertSameEpoch(long expected, Instant actualInstant, String message) {
        assertEquals(expected, actualInstant.toEpochMilli(), message);
    }

    public static void assertSameEpoch(Instant expected, Long actual, String message) {
        assertThat(message, actual, sameEpochMillis(expected));
    }

    public static void assertSameEpoch(ZonedDateTime expected, Long actual, String message) {
        assertThat(message, actual, sameEpochMillis(expected));
    }

    public static void assertSameEpoch(String expected, Long actual, String message) {
        assertThat(message, actual, sameEpochMillis(expected));
    }
}
