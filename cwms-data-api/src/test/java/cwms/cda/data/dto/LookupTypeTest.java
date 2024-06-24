/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto;

import cwms.cda.api.errors.FieldException;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public final class LookupTypeTest {

    @Test
    void createLookupType_allFieldsProvided_success() {
        LookupType item = new LookupType.Builder()
                .withOfficeId("Office123")
                .withDisplayValue("Example Display Value")
                .withTooltip("This is a tooltip for the lookup type.")
                .withActive(true)
                .build();
        assertAll(() -> assertEquals("Office123", item.getOfficeId(), "The office ID does not match the provided value"),
                () -> assertEquals("Example Display Value", item.getDisplayValue(), "The display value does not match the provided value"),
                () -> assertEquals("This is a tooltip for the lookup type.", item.getTooltip(), "The tooltip does not match the provided value"),
                () -> assertTrue(item.getActive(), "The active status does not match the provided value"));
    }

    @Test
    void createLookupType_missingField_throwsFieldException() {
        assertAll(
                // When Office ID is missing
                () -> assertThrows(FieldException.class, () -> {
                    LookupType item = new LookupType.Builder()
                            .withDisplayValue("Example Display Value")
                            .withTooltip("This is a tooltip for the lookup type.")
                            .withActive(true)
                            .build();
                    item.validate();
                }, "The validate method should have thrown a FieldException because the office ID field is missing"),

                // When Display Value is missing
                () -> assertThrows(FieldException.class, () -> {
                    LookupType item = new LookupType.Builder()
                            .withOfficeId("Office123")
                            .withTooltip("This is a tooltip for the lookup type.")
                            .withActive(true)
                            .build();
                    item.validate();
                }, "The validate method should have thrown a FieldException because the display value field is missing"));
    }

    @Test
    void createLookupType_serialize_roundtrip() {
        LookupType lookupType = new LookupType.Builder()
                .withOfficeId("Office123")
                .withDisplayValue("Example Display Value")
                .withTooltip("This is a tooltip for the lookup type.")
                .withActive(true)
                .build();
        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, lookupType);
        LookupType deserialized = Formats.parseContent(contentType, json, LookupType.class);
        assertSame(lookupType, deserialized);
    }

    @Test
    void createLookupType_deserialize() throws Exception {
        LookupType lookupType = new LookupType.Builder()
                .withOfficeId("Office123")
                .withDisplayValue("Example Display Value")
                .withTooltip("This is a tooltip for the lookup type.")
                .withActive(true)
                .build();
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/lookup_type.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        LookupType deserialized = Formats.parseContent(contentType, json, LookupType.class);
        assertSame(lookupType, deserialized);
    }

    public static void assertSame(LookupType lookupType, LookupType deserialized) {
        assertEquals(lookupType.getOfficeId(), deserialized.getOfficeId());
        assertEquals(lookupType.getDisplayValue(), deserialized.getDisplayValue());
        assertEquals(lookupType.getTooltip(), deserialized.getTooltip());
        assertEquals(lookupType.getActive(), deserialized.getActive());
    }
}