package cwms.cda.helpers;

import static org.junit.jupiter.api.Assertions.*;

import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;

class ReplaceUtilsTest {

    @Test
    void replace() {
        String expected = "testValue";
        assertEquals(expected, ReplaceUtils.replace("{key}", "{key}", expected));
        assertEquals(expected, ReplaceUtils.replace("{clob-id}", "{clob-id}", expected));
        assertEquals("Your id is " + expected, ReplaceUtils.replace("Your id is {clob-id}", "{clob-id}", expected));

    }

    @Test
    void replaceWithEncoding() {
        String expected = "test+Value";
        assertEquals(expected, ReplaceUtils.replace("{key}", "{key}", "test Value"));
        assertEquals(expected, ReplaceUtils.replace("{clob-id}", "{clob-id}", "test Value"));
        String value = "/TIME SERIES TEXT/1978044";
        expected = "%2FTIME+SERIES+TEXT%2F1978044";
        assertEquals(expected, ReplaceUtils.replace("{clob-id}", "{clob-id}", value));
    }

    //Additional Tests
    @Test
    void replaceWithNullTemplate() {
        assertThrows(NullPointerException.class, () -> ReplaceUtils.replace(null, "key", "testValue"));
    }

    @Test
    void replaceWithNullKey() {
        assertEquals("{key}", ReplaceUtils.replace("{key}", null, "testValue"));
    }

    @Test
    void replaceWithNullBoth() {
        assertNull(ReplaceUtils.replace(null, null, "testValue"));
    }

    @Test
    void replaceClobExample(){

        String template = "{context-root}clob/ignored?clob-id={clob-id}&office-id={office}";

        template = ReplaceUtils.replace(template, "{context-root}", "http://localhost:7000/spk-data/", false);
        template = ReplaceUtils.replace(template, "{office}", "SPK");
        template = ReplaceUtils.replace(template, "{clob-id}", "/TIME SERIES TEXT/1978044");

        assertEquals("http://localhost:7000/spk-data/clob/ignored?clob-id=%2FTIME+SERIES+TEXT%2F1978044&office-id=SPK", template);
    }

    @Test
    void testOperator(){
        String template = "{context-root}clob/ignored?clob-id={clob-id}&office-id={office}";
        UnaryOperator<String> mapper = ReplaceUtils.replace(template, "{clob-id}");
        mapper = ReplaceUtils.alsoReplace(mapper, "{context-root}", "http://localhost:7000/spk-data/", false);
        mapper = ReplaceUtils.alsoReplace(mapper, "{office}", "SPK");
        assertEquals("http://localhost:7000/spk-data/clob/ignored?clob-id=%2FTIME+SERIES+TEXT%2F1978044&office-id=SPK", mapper.apply("/TIME SERIES TEXT/1978044"));
    }

    @Test
    void testOperatorBuilder(){
        String template = "{context-root}clob/ignored?clob-id={clob-id}&office-id={office}";

        // Initial setup
        ReplaceUtils.OperatorBuilder builder = new ReplaceUtils.OperatorBuilder();
        builder.withTemplate(template);
        builder.withOperatorKey("{clob-id}");

        // Then dao or controller could set these bits:
        builder.replace("{context-root}", "http://localhost:7000/spk-data/", false);
        builder.replace("{office}", "SPK");

        UnaryOperator<String> urlBuilder = builder.build();

        assertEquals("http://localhost:7000/spk-data/clob/ignored?clob-id=%2FTIME+SERIES+TEXT%2F1978044&office-id=SPK", urlBuilder.apply("/TIME SERIES TEXT/1978044"));

        // Can call multiple times with different clob-ids
        assertEquals("http://localhost:7000/spk-data/clob/ignored?clob-id=%2FTIME+SERIES+TEXT%2F000000&office-id=SPK", urlBuilder.apply("/TIME SERIES TEXT/000000"));
    }

    @Test
    void testOperatorBuilderExtraKey(){
        String template = "http://localhost:7000/spk-data/clob/ignored?clob-id={clob-id}&office-id={office}";

        // Initial setup
        ReplaceUtils.OperatorBuilder builder = new ReplaceUtils.OperatorBuilder();
        builder.withTemplate(template);
        builder.withOperatorKey("{clob-id}");

        // context-root is no longer in the template - this shouldn't cause an error
        builder.replace("{context-root}", "http://localhost:7000/spk-data/", false);
        builder.replace("{office}", "SPK");

        UnaryOperator<String> urlBuilder = builder.build();

        assertEquals("http://localhost:7000/spk-data/clob/ignored?clob-id=%2FTIME+SERIES+TEXT%2F1978044&office-id=SPK", urlBuilder.apply("/TIME SERIES TEXT/1978044"));

        // Can call multiple times with different clob-ids
        assertEquals("http://localhost:7000/spk-data/clob/ignored?clob-id=%2FTIME+SERIES+TEXT%2F000000&office-id=SPK", urlBuilder.apply("/TIME SERIES TEXT/000000"));
    }

}