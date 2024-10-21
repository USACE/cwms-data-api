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

import cwms.cda.data.dto.measurement.SupplementalStreamflowMeasurement;
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

final class SupplementalStreamflowMeasurementTest {

    @Test
    void createSupplementalStreamflowMeasurement_allFieldsProvided_success() {
        SupplementalStreamflowMeasurement item = new SupplementalStreamflowMeasurement.Builder()
                .withChannelFlow(300.0)
                .withOverbankFlow(50.0)
                .withOverbankMaxDepth(5.0)
                .withChannelMaxDepth(10.0)
                .withAvgVelocity(1.5)
                .withSurfaceVelocity(2.0)
                .withMaxVelocity(3.0)
                .withEffectiveFlowArea(200.0)
                .withCrossSectionalArea(250.0)
                .withMeanGage(20.0)
                .withTopWidth(30.0)
                .withMainChannelArea(150.0)
                .withOverbankArea(80.0)
                .build();

        assertAll(
                () -> assertEquals(300.0, item.getChannelFlow(), "Channel Flow"),
                () -> assertEquals(50.0, item.getOverbankFlow(), "Overbank Flow"),
                () -> assertEquals(5.0, item.getOverbankMaxDepth(), "Overbank Max Depth"),
                () -> assertEquals(10.0, item.getChannelMaxDepth(), "Channel Max Depth"),
                () -> assertEquals(1.5, item.getAvgVelocity(), "Avg Velocity"),
                () -> assertEquals(2.0, item.getSurfaceVelocity(), "Surface Velocity"),
                () -> assertEquals(3.0, item.getMaxVelocity(), "Max Velocity"),
                () -> assertEquals(200.0, item.getEffectiveFlowArea(), "Effective Flow Area"),
                () -> assertEquals(250.0, item.getCrossSectionalArea(), "Cross-Sectional Area"),
                () -> assertEquals(20.0, item.getMeanGage(), "Mean Gage"),
                () -> assertEquals(30.0, item.getTopWidth(), "Top Width"),
                () -> assertEquals(150.0, item.getMainChannelArea(), "Main Channel Area"),
                () -> assertEquals(80.0, item.getOverbankArea(), "Overbank Area")
        );
    }

    @Test
    void createSupplementalStreamflowMeasurement_serialize_roundtrip() {
        SupplementalStreamflowMeasurement measurement = new SupplementalStreamflowMeasurement.Builder()
                .withChannelFlow(300.0)
                .withOverbankFlow(50.0)
                .withOverbankMaxDepth(5.0)
                .withChannelMaxDepth(10.0)
                .withAvgVelocity(1.5)
                .withSurfaceVelocity(2.0)
                .withMaxVelocity(3.0)
                .withEffectiveFlowArea(200.0)
                .withCrossSectionalArea(250.0)
                .withMeanGage(20.0)
                .withTopWidth(30.0)
                .withMainChannelArea(150.0)
                .withOverbankArea(80.0)
                .build();

        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, measurement);
        SupplementalStreamflowMeasurement deserialized = Formats.parseContent(contentType, json, SupplementalStreamflowMeasurement.class);

        DTOMatch.assertMatch(measurement, deserialized);
    }

    @Test
    void createSupplementalStreamflowMeasurement_deserialize() throws Exception {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/supplemental_streamflow_measurement.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        SupplementalStreamflowMeasurement deserialized = Formats.parseContent(contentType, json, SupplementalStreamflowMeasurement.class);

        SupplementalStreamflowMeasurement expectedMeasurement = new SupplementalStreamflowMeasurement.Builder()
                .withChannelFlow(300.0)
                .withOverbankFlow(50.0)
                .withOverbankMaxDepth(5.0)
                .withChannelMaxDepth(10.0)
                .withAvgVelocity(1.5)
                .withSurfaceVelocity(2.0)
                .withMaxVelocity(3.0)
                .withEffectiveFlowArea(200.0)
                .withCrossSectionalArea(250.0)
                .withMeanGage(20.0)
                .withTopWidth(30.0)
                .withMainChannelArea(150.0)
                .withOverbankArea(80.0)
                .build();

        DTOMatch.assertMatch(expectedMeasurement, deserialized);
    }
}
