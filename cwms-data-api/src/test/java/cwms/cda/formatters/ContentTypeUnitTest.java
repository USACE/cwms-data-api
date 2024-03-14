package cwms.cda.formatters;

import cwms.cda.formatters.ContentType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ContentTypeUnitTest {

    /**
     * This Class is used to test the compareTo() method in the ContentType class.
     * The compareTo() method compares the current instance with another instance.
     */
     
    @Test
    void testCompareToEqual() {
        ContentType contentType1 = new ContentType("text/html;q=0.7");
        ContentType contentType2 = new ContentType("text/html;q=0.7");
        assertEquals(0, contentType1.compareTo(contentType2));
    }

    @Test
    void testCompareToGreater() {
        ContentType contentType1 = new ContentType("text/html;q=0.9");
        ContentType contentType2 = new ContentType("text/html;q=0.7");
        assertEquals(1, contentType1.compareTo(contentType2));
    }

    @Test
    void testCompareToLess() {
        ContentType contentType1 = new ContentType("text/html;q=0.7");
        ContentType contentType2 = new ContentType("text/html;q=0.9");
        assertEquals(-1, contentType1.compareTo(contentType2));
    }
}