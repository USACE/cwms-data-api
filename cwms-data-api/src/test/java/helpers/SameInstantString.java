package helpers;

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

public class SameInstantString extends TypeSafeMatcher<String> {
    private final Instant expected;


    public SameInstantString(Instant expected) {
        this.expected = expected;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(" must parse to the same Instant as " + expected);
    }

    @Override
    protected boolean matchesSafely(String inputStr) {

        Instant inputInstant;
        try {
            inputInstant = jacksonParse(inputStr);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        return inputInstant.equals(expected);
    }

    public static Instant jacksonParse(String inputStr) throws JsonProcessingException {
        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        ObjectReader objectReader = objectMapper.readerFor(Holder.class);
        Holder obj = objectReader.readValue("{\"instant\":\"" + inputStr + "\"}");
        return obj.instant;
    }

    @Factory
    public static Matcher<String> sameInstant(String expectedStr) {
        Instant instant;

//        //Parse expectedStr into Instant using DateUtils.
//        - Not doing this b/c focussing on using from RestAssured.
//        ZonedDateTime expectedZdt = DateUtils.parseUserDate(expectedStr, "UTC");
//        instant = expectedZdt.toInstant();

        try {
            instant = jacksonParse(expectedStr);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return new SameInstantString(instant);
    }

    @Factory
    public static Matcher<String> sameInstant(long expected) {
        return sameInstant(String.valueOf(expected));
    }

    @Factory
    public static Matcher<String> sameInstant(Instant expectedInstant) {
        return new SameInstantString(expectedInstant);
    }

    @Factory
    public static Matcher<String> sameInstant(ZonedDateTime expectedZdt) {
        return new SameInstantString(expectedZdt.toInstant());
    }


    private static class Holder {
        public Holder() {
        }

        public Instant instant;
    }
}
