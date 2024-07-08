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

public final class CwmsIdTest {

    @Test
    void createLocationIdentifier_allFieldsProvided_success() {
        CwmsId item = new CwmsId.Builder()
                .withName("Stream123")
                .withOfficeId("Office123")
                .build();
        assertAll(() -> assertEquals("Stream123", item.getName(), "The location ID does not match the provided value"),
                () -> assertEquals("Office123", item.getOfficeId(), "The office ID does not match the provided value"));
    }

    @Test
    void createLocationIdentifier_missingField_throwsFieldException() {
        assertAll(
                // When locationId is missing
                () -> assertThrows(FieldException.class, () -> {
                    CwmsId item = new CwmsId.Builder()
                            .withOfficeId("Office123")
                            .build();
                    item.validate();
                }, "The validate method should have thrown a FieldException because the locationId field is missing"),

                // When officeId is missing
                () -> assertThrows(FieldException.class, () -> {
                    CwmsId item = new CwmsId.Builder()
                            .withName("Stream123")
                            .build();
                    item.validate();
                }, "The validate method should have thrown a FieldException because the officeId field is missing"));
    }

    @Test
    void createLocationIdentifier_serialize_roundtrip() {
        CwmsId cwmsId = new CwmsId.Builder()
                .withName("Stream123")
                .withOfficeId("Office123")
                .build();
        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, cwmsId);
        CwmsId deserialized = Formats.parseContent(contentType, json, CwmsId.class);
        assertSame(cwmsId, deserialized);
    }

    @Test
    void createLocationIdentifier_deserialize() throws Exception {
        CwmsId cwmsId = new CwmsId.Builder()
                .withName("Stream123")
                .withOfficeId("Office123")
                .build();
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/location_identifier.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        CwmsId deserialized = Formats.parseContent(contentType, json, CwmsId.class);
        assertSame(cwmsId, deserialized);
    }

    @Test
    void createLocationIdentifier_verifyOrdering() {
        CwmsId cwmsId = new CwmsId.Builder()
                .withName("Stream123")
                .withOfficeId("Office123")
                .build();

        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, cwmsId);

        // Verify that officeId comes before locationId
        int officeIdIndex = json.indexOf("\"office-id\"");
        int locationIdIndex = json.indexOf("\"name\"");

        assertTrue(officeIdIndex < locationIdIndex, "The officeId field should come before the locationId field in the JSON string");
    }

    public static void assertSame(CwmsId first, CwmsId second, String variableName) {
        assertAll(
                () -> assertEquals(first.getOfficeId(), second.getOfficeId(), variableName + " is not the same. Office ID differs"),
                () -> assertEquals(first.getName(), second.getName(), variableName + " is not the same. Name differs")
        );
    }

    public static void assertSame(CwmsId first, CwmsId second) {
        assertSame(first, second, "LocationIdentifier");
    }
}