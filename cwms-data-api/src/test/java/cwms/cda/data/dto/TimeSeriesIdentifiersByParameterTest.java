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
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

import cwms.cda.api.errors.FieldException;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DTOMatch;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

final class TimeSeriesIdentifiersByParameterTest {

    @Test
    void createPublishedTimeSeriesData_allFieldsProvided_success() {
        TimeSeriesMetaData ts1 = new TimeSeriesMetaData.Builder()
                .withTsId(new TimeSeriesIdentifierDescriptor.Builder()
                        .withTimeSeriesId("AARK.Stage.Inst.15Minutes.0.Ccp-Rev")
                        .withOfficeId("SWT")
                        .withActive(true)
                        .withIntervalOffsetMinutes(0L)
                        .withZoneId(ZoneId.of("UTC"))
                        .build())
                .withNotes("This is a test")
                .withDateRefreshed(Instant.parse("2025-03-03T12:00:00Z")) // Adding date refreshed
                .build();

        TimeSeriesMetaData ts2 = new TimeSeriesMetaData.Builder()
                .withTsId(new TimeSeriesIdentifierDescriptor.Builder()
                        .withTimeSeriesId("AARK.Flow.Inst.15Minutes.0.Ccp-Rev")
                        .withOfficeId("SWT")
                        .withActive(true)
                        .withIntervalOffsetMinutes(0L)
                        .withZoneId(ZoneId.of("UTC"))
                        .build())
                .withNotes("This is another test")
                .withDateRefreshed(Instant.parse("2025-03-03T12:00:00Z")) // Adding date refreshed
                .build();

        CwmsId locId = new CwmsId.Builder()
                .withName("AARK")
                .withOfficeId("SWT")
                .build();
        TimeSeriesIdentifiersByParameter items = new TimeSeriesIdentifiersByParameter.Builder()
                .withLocationId(locId)
                .withBoundingOfficeId("SWT")
                .withKind("SITE")
                .withDateRefreshed(Instant.parse("2025-03-03T12:00:00Z"))
                .withNotes("General notes")
                .withTimeSeriesId("STAGE", ts1)
                .withTimeSeriesId("OUTFLOW", ts2)
                .build();

        assertAll(
                () -> DTOMatch.assertMatch(locId, items.getLocationId()),
                () -> assertEquals("SITE", items.getKind()),
                () -> assertEquals("SWT", items.getBoundingOfficeId()),
                () -> assertEquals(Instant.parse("2025-03-03T12:00:00Z"), items.getDateRefreshed()),
                () -> assertEquals("General notes", items.getNotes()),
                () -> DTOMatch.assertMatch(ts1, items.getTimeSeriesIdsByParameter().get("STAGE")),
                () -> DTOMatch.assertMatch(ts2, items.getTimeSeriesIdsByParameter().get("OUTFLOW"))
        );
    }

    @Test
    void createPublishedTimeSeriesData_missingField_throwsFieldException() {
        TimeSeriesMetaData tsDescriptor = new TimeSeriesMetaData.Builder()
                .withTsId(new TimeSeriesIdentifierDescriptor.Builder()
                        .withTimeSeriesId("AARK.Stage.Inst.15Minutes.0.Ccp-Rev")
                        .withOfficeId("SWT")
                        .withActive(true)
                        .withIntervalOffsetMinutes(0L)
                        .withZoneId(ZoneId.of("UTC"))
                        .build())
                .withNotes("This is a test")
                .withDateRefreshed(Instant.now()) // Adding date refreshed
                .build();

        Map<String, TimeSeriesMetaData> typeToTsIdMap = new HashMap<>();
        typeToTsIdMap.put("STAGE", tsDescriptor);

        assertAll(
                () -> assertThrows(FieldException.class, () -> {
                    TimeSeriesIdentifiersByParameter item = new TimeSeriesIdentifiersByParameter.Builder()
                            .withTimeSeriesIdsByParameter(typeToTsIdMap)
                            .build();
                    item.validate();
                }, "The validate method should have thrown a FieldException because the location ID is missing")
        );
    }

    @Test
    void createPublishedTimeSeriesData_serialize_roundtrip() {
        TimeSeriesMetaData tsDescriptor = new TimeSeriesMetaData.Builder()
                .withTsId(new TimeSeriesIdentifierDescriptor.Builder()
                        .withTimeSeriesId("AARK.Stage.Inst.15Minutes.0.Ccp-Rev")
                        .withOfficeId("SWT")
                        .withActive(true)
                        .withIntervalOffsetMinutes(0L)
                        .withZoneId(ZoneId.of("UTC"))
                        .build())
                .withNotes("This is a test")
                .withDateRefreshed(Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS)) // Truncating to avoid precision mismatch
                .build();
        TimeSeriesIdentifiersByParameter data = new TimeSeriesIdentifiersByParameter.Builder()
                .withLocationId(new CwmsId.Builder()
                        .withName("AARK")
                        .withOfficeId("SWT")
                        .build())
                .withBoundingOfficeId("SWT")
                .withKind("SITE")
                .withTimeSeriesId("STAGE", tsDescriptor)
                .build();

        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, data);
        TimeSeriesIdentifiersByParameter deserialized = Formats.parseContent(contentType, json, TimeSeriesIdentifiersByParameter.class);
        DTOMatch.assertMatch(data, deserialized);
    }

    @Test
    void createPublishedTimeSeriesData_deserialize_multipleEntries() throws Exception {
        // Expected data
        TimeSeriesMetaData ts1Stage = new TimeSeriesMetaData.Builder()
                .withTsId(new TimeSeriesIdentifierDescriptor.Builder()
                        .withTimeSeriesId("AARK.Stage.Inst.15Minutes.0.Ccp-Rev")
                        .withOfficeId("SWT")
                        .withActive(true)
                        .withIntervalOffsetMinutes(0L)
                        .withZoneId(ZoneId.of("UTC"))
                        .build())
                .withNotes("Stage data for AARK")
                .withDateRefreshed(Instant.parse("2025-03-03T12:00:00Z"))
                .build();

        TimeSeriesMetaData ts2Outflow = new TimeSeriesMetaData.Builder()
                .withTsId(new TimeSeriesIdentifierDescriptor.Builder()
                        .withTimeSeriesId("AARK.Flow.Inst.1Hour.0.Ccp-Rev")
                        .withOfficeId("SWT")
                        .withActive(true)
                        .withIntervalOffsetMinutes(0L)
                        .withZoneId(ZoneId.of("UTC"))
                        .build())
                .withNotes("Flow data for AARK")
                .withDateRefreshed(Instant.parse("2025-03-03T12:00:00Z"))
                .build();

        // Expected data for locations
        TimeSeriesIdentifiersByParameter expectedAARK = new TimeSeriesIdentifiersByParameter.Builder()
                .withLocationId(new CwmsId.Builder()
                        .withName("AARK")
                        .withOfficeId("SWT")
                        .build())
                .withBoundingOfficeId("SWT")
                .withKind("SITE")
                .withTimeSeriesId("STAGE", ts1Stage)
                .withTimeSeriesId("OUTFLOW", ts2Outflow)
                .build();

        TimeSeriesIdentifiersByParameterList list = new TimeSeriesIdentifiersByParameterList.Builder()
                .withCursor("cursor")
                .withPageSize(20)
                .withTotal(100)
                .withTimeSeriesIdsForLocations(Arrays.asList(expectedAARK))
                .build();

        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/time_series_identifiers_by_parameter.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);

        ContentType contentType = new ContentType(Formats.JSON);
        TimeSeriesIdentifiersByParameterList deserialized = Formats.parseContent(contentType, json, TimeSeriesIdentifiersByParameterList.class);

        // Verify the deserialized data matches the expected data
        assertAll(() -> DTOMatch.assertMatch(list, deserialized));
    }

}
