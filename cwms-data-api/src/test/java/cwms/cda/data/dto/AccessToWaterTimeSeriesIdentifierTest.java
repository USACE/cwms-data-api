/*
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
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
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto;

import java.time.ZoneId;
import static org.junit.jupiter.api.Assertions.*;

import cwms.cda.api.errors.FieldException;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DTOMatch;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

final class AccessToWaterTimeSeriesIdentifierTest {

    @Test
    void createAccessToWaterTimeSeriesData_allFieldsProvided_success() {
        TimeSeriesIdentifierDescriptor tsDescriptor = new TimeSeriesIdentifierDescriptor.Builder()
                .withTimeSeriesId("VANL.Stage.Inst.15Minutes.0.Ccp-Rev")
                .withOfficeId("SWT")
                .withActive(true)
                .withIntervalOffsetMinutes(0L)
                .withZoneId(ZoneId.of("UTC"))
                .build();
        AccessToWaterTimeSeriesIdentifier item = new AccessToWaterTimeSeriesIdentifier.Builder()
                .withOfficeId("SWT")
                .withLocationId("VANL")
                .withTimeSeriesIdDescriptor(tsDescriptor)
                .withTsType("STAGE")
                .build();

        assertAll(
                () -> assertEquals("VANL", item.getLocationId(), "The location ID does not match the provided value"),
                () -> DTOMatch.assertMatch(tsDescriptor, item.getTimeSeriesIdDescriptor()),
                () -> assertEquals("STAGE", item.getTsType(), "The time series type does not match the provided value")
        );
    }

    @Test
    void createAccessToWaterTimeSeriesData_missingField_throwsFieldException() {
        TimeSeriesIdentifierDescriptor tsDescriptor = new TimeSeriesIdentifierDescriptor.Builder()
                .withTimeSeriesId("VANL.Stage.Inst.15Minutes.0.Ccp-Rev")
                .withOfficeId("SWT")
                .withActive(true)
                .withIntervalOffsetMinutes(0L)
                .withZoneId(ZoneId.of("UTC"))
                .build();
        assertAll(
                () -> assertThrows(FieldException.class, () -> {
                    AccessToWaterTimeSeriesIdentifier item = new AccessToWaterTimeSeriesIdentifier.Builder()
                            .withOfficeId("SWT")
                            .withTimeSeriesIdDescriptor(tsDescriptor)
                            .withTsType("STAGE")
                            .build();
                    item.validate();
                }, "The validate method should have thrown a FieldException because the location ID is missing"),

                () -> assertThrows(FieldException.class, () -> {
                    AccessToWaterTimeSeriesIdentifier item = new AccessToWaterTimeSeriesIdentifier.Builder()
                            .withOfficeId("SWT")
                            .withLocationId("VANL")
                            .withTsType("STAGE")
                            .build();
                    item.validate();
                }, "The validate method should have thrown a FieldException because the TimeSeries ID is missing"),

                () -> assertThrows(FieldException.class, () -> {
                    AccessToWaterTimeSeriesIdentifier item = new AccessToWaterTimeSeriesIdentifier.Builder()
                            .withOfficeId("SWT")
                            .withLocationId("VANL")
                            .withTimeSeriesIdDescriptor(tsDescriptor)
                            .build();
                    item.validate();
                }, "The validate method should have thrown a FieldException because the time series type is missing"),

                () -> assertThrows(FieldException.class, () -> {
                    AccessToWaterTimeSeriesIdentifier item = new AccessToWaterTimeSeriesIdentifier.Builder()
                            .withLocationId("VANL")
                            .withTimeSeriesIdDescriptor(tsDescriptor)
                            .withTsType("STAGE")
                            .build();
                    item.validate();
                }, "The validate method should have thrown a FieldException because the office id is missing")
        );
    }

    @Test
    void createAccessToWaterTimeSeriesData_serialize_roundtrip() {
        TimeSeriesIdentifierDescriptor tsDescriptor = new TimeSeriesIdentifierDescriptor.Builder()
                .withTimeSeriesId("VANL.Stage.Inst.15Minutes.0.Ccp-Rev")
                .withOfficeId("SWT")
                .withActive(true)
                .withIntervalOffsetMinutes(0L)
                .withZoneId(ZoneId.of("UTC"))
                .build();
        AccessToWaterTimeSeriesIdentifier data = new AccessToWaterTimeSeriesIdentifier.Builder()
                .withOfficeId("SWT")
                .withLocationId("VANL")
                .withTimeSeriesIdDescriptor(tsDescriptor)
                .withTsType("STAGE")
                .build();

        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, data);
        AccessToWaterTimeSeriesIdentifier deserialized = Formats.parseContent(contentType, json, AccessToWaterTimeSeriesIdentifier.class);
        DTOMatch.assertMatch(data, deserialized);
    }

    @Test
    void createAccessToWaterTimeSeriesData_deserialize() throws Exception {
        TimeSeriesIdentifierDescriptor tsDescriptor = new TimeSeriesIdentifierDescriptor.Builder()
                .withTimeSeriesId("VANL.Stage.Inst.15Minutes.0.Ccp-Rev")
                .withOfficeId("SWT")
                .withActive(true)
                .withIntervalOffsetMinutes(0L)
                .withZoneId(ZoneId.of("UTC"))
                .build();
        AccessToWaterTimeSeriesIdentifier expected = new AccessToWaterTimeSeriesIdentifier.Builder()
                .withOfficeId("SWT")
                .withLocationId("VANL")
                .withTimeSeriesIdDescriptor(tsDescriptor)
                .withTsType("STAGE")
                .build();

        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/access_to_water_time_series_data.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        AccessToWaterTimeSeriesIdentifier deserialized = Formats.parseContent(contentType, json, AccessToWaterTimeSeriesIdentifier.class);
        DTOMatch.assertMatch(expected, deserialized);
    }
}
