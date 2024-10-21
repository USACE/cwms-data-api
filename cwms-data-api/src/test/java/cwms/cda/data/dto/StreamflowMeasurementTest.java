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

import cwms.cda.data.dto.measurement.StreamflowMeasurement;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DTOMatch;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.Test;

final class StreamflowMeasurementTest {

    @Test
    void createStreamflowMeasurement_allFieldsProvided_success() {
        StreamflowMeasurement item = new StreamflowMeasurement.Builder()
                .withGageHeight(5.5)
                .withFlow(250.0)
                .withQuality("Good")
                .build();

        assertAll(
                () -> assertEquals(5.5, item.getGageHeight(), "Gage Height"),
                () -> assertEquals(250.0, item.getFlow(), "Flow"),
                () -> assertEquals("Good", item.getQuality(), "Quality")
        );
    }

    @Test
    void createStreamflowMeasurement_serialize_roundtrip() {
        StreamflowMeasurement measurement = new StreamflowMeasurement.Builder()
                .withGageHeight(5.5)
                .withFlow(250.0)
                .withQuality("Good")
                .build();

        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, measurement);
        StreamflowMeasurement deserialized = Formats.parseContent(contentType, json, StreamflowMeasurement.class);

        DTOMatch.assertMatch(measurement, deserialized);
    }

    @Test
    void createStreamflowMeasurement_deserialize() throws Exception {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/streamflow_measurement.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        StreamflowMeasurement deserialized = Formats.parseContent(contentType, json, StreamflowMeasurement.class);

        StreamflowMeasurement expectedMeasurement = new StreamflowMeasurement.Builder()
                .withGageHeight(5.5)
                .withFlow(250.0)
                .withQuality("Good")
                .build();

        DTOMatch.assertMatch(expectedMeasurement, deserialized);
    }
}
