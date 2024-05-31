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

import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.api.errors.FieldException;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

final class PropertyTest {

    @Test
    void createProperty_allFieldsProvided_success() {
        Property item = new Property.Builder()
                .withCategory("TestCategory")
                .withOffice("TestOffice")
                .withName("TestName")
                .withValue("TestValue")
                .build();
        assertAll(() -> assertEquals("TestCategory", item.getCategory(), "The category does not match the provided value"),
                () -> assertEquals("TestOffice", item.getOffice(), "The office does not match the provided value"),
                () -> assertEquals("TestName", item.getName(), "The name does not match the provided value"),
                () -> assertEquals("TestValue", item.getValue(), "The value does not match the provided value"));
    }

    @Test
    void createProperty_missingField_throwsFieldException() {
        assertAll(
                // When Office is missing
                () -> assertThrows(FieldException.class, () -> {
                    Property item = new Property.Builder()
                            .withCategory("TestCategory")
                            // missing Office
                            .withName("TestName")
                            .withValue("TestValue")
                            .build();
                    item.validate();
                }, "The validate method should have thrown a FieldException because the office field is missing"),

                // When Category is missing
                () -> assertThrows(FieldException.class, () -> {
                    Property item = new Property.Builder()
                            // missing Category
                            .withOffice("TestOffice")
                            .withName("TestName")
                            .withValue("TestValue")
                            .build();
                    item.validate();
                }, "The validate method should have thrown a FieldException because the category field is missing"),

                // When Name is missing
                () -> assertThrows(FieldException.class, () -> {
                    Property item = new Property.Builder()
                            .withCategory("TestCategory")
                            .withOffice("TestOffice")
                            // missing Name
                            .withValue("TestValue")
                            .build();
                    item.validate();
                }, "The validate method should have thrown a FieldException because the name field is missing"),
                // When Value is missing
                () -> assertThrows(FieldException.class, () -> new Property.Builder()
                        .withCategory("TestCategory")
                        .withOffice("TestOffice")
                        .withName("TestName")
                        // missing value
                        .build()
                        .validate(), "The validate method should have thrown a FieldException because the value field is missing"));
    }

    @Test
    void createProperty_serialize_roundtrip() throws Exception {
        Property property = new Property.Builder()
                .withCategory("TestCategory")
                .withOffice("TestOffice")
                .withName("TestName")
                .withValue("TestValue")
                .build();
        String json = Formats.format(new ContentType(Formats.JSONV2), property);
        ObjectMapper om = JsonV2.buildObjectMapper();
        Property deserialized = om.readValue(json, Property.class);
        assertEquals(property, deserialized, "Property deserialized from JSON doesn't equal original");
    }

    @Test
    void createProperty_deserialize() throws Exception {
        Property property = new Property.Builder()
                .withCategory("TestCategory")
                .withOffice("TestOffice")
                .withName("TestName")
                .withValue("TestValue")
                .build();
        ObjectMapper om = JsonV2.buildObjectMapper();
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/property.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        Property deserialized = om.readValue(json, Property.class);
        assertEquals(property, deserialized, "Property deserialized from JSON doesn't equal original");
    }
}
