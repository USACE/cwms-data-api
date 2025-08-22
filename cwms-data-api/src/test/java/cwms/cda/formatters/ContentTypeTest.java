package cwms.cda.formatters;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ContentTypeTest {

    @Test
    void test_ctor() {
        ContentType ct = new ContentType("application/json");
        assertEquals("application/json", ct.getType());
        Map<String, String> parameters = ct.getParameters();
        assertTrue(parameters == null || parameters.isEmpty());
    }

    @Test
    void test_ctor_null() {
        assertThrows(NullPointerException.class, () -> {
            new ContentType(null);
        });
    }

    @Test
    void test_ctor_empty() {
        ContentType ct = new ContentType("");
        Map<String, String> parameters = ct.getParameters();
        assertTrue(parameters == null || parameters.isEmpty());
        assertNull(ct.getCharset());
    }

    @Test
    void test_ctor_garbage() {
        ContentType ct = new ContentType("qawicxqyjx");
        Map<String, String> parameters = ct.getParameters();
        assertTrue(parameters == null || parameters.isEmpty());
        assertNull(ct.getCharset());
    }


    @Test
    void test_ctor_w_charset() {
        ContentType ct = new ContentType("application/json;charset=UTF-8");
        assertEquals("application/json", ct.getType());
        Map<String, String> parameters = ct.getParameters();
        assertTrue(parameters == null || parameters.isEmpty());
        assertEquals("UTF-8", ct.getCharset());
    }

    @Test
    void test_ctor_w_charset_space() {
        ContentType ct = new ContentType("application/json; charset=UTF-8");
        assertEquals("application/json", ct.getType());
        Map<String, String> parameters = ct.getParameters();
        assertTrue(parameters == null || parameters.isEmpty());

        assertEquals("UTF-8", ct.getCharset());
    }


    @ParameterizedTest
    @CsvSource(value = {
            "application/json;charset=utf-8|application/json;",
            "application/json;version=2,charset=utf-8|application/json;version=2"
    }, delimiter = '|')
    void test_equals_and_equivalent(String a, String b) {
        ContentType aCt = new ContentType(a);
        ContentType bCt = new ContentType(b);

        assertTrue(ContentType.equivalent(a, b));
        assertTrue(aCt.equals(bCt));
    }
}