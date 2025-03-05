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

final class TimeSeriesIdentifiersByTypeTest {

    @Test
    void createAccessToWaterTimeSeriesData_allFieldsProvided_success() {
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
                .withBoundingOfficeId("SWT")
                .withKind("SITE")
                .build();
        TimeSeriesIdentifiersByType items = new TimeSeriesIdentifiersByType.Builder()
                .withLocationId(locId)
                .withTimeSeriesId("STAGE", ts1)
                .withTimeSeriesId("OUTFLOW", ts2)
                .build();

        assertAll(
                () -> DTOMatch.assertMatch(locId, items.getLocationId()),
                () -> DTOMatch.assertMatch(ts1, items.getTimeSeriesIds().get("STAGE")),
                () -> DTOMatch.assertMatch(ts2, items.getTimeSeriesIds().get("OUTFLOW"))
        );
    }

    @Test
    void createAccessToWaterTimeSeriesData_missingField_throwsFieldException() {
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
                    TimeSeriesIdentifiersByType item = new TimeSeriesIdentifiersByType.Builder()
                            .withTimeSeriesIds(typeToTsIdMap)
                            .build();
                    item.validate();
                }, "The validate method should have thrown a FieldException because the location ID is missing")
        );
    }

    @Test
    void createAccessToWaterTimeSeriesData_serialize_roundtrip() {
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
        TimeSeriesIdentifiersByType data = new TimeSeriesIdentifiersByType.Builder()
                .withLocationId(new CwmsId.Builder()
                        .withName("AARK")
                        .withOfficeId("SWT")
                        .withBoundingOfficeId("SWT")
                        .withKind("SITE")
                        .build())
                .withTimeSeriesId("STAGE", tsDescriptor)
                .build();

        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, data);
        TimeSeriesIdentifiersByType deserialized = Formats.parseContent(contentType, json, TimeSeriesIdentifiersByType.class);
        DTOMatch.assertMatch(data, deserialized);
    }

    @Test
    void createAccessToWaterTimeSeriesData_deserialize_multipleEntries() throws Exception {
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

        TimeSeriesMetaData ts3Precip = new TimeSeriesMetaData.Builder()
                .withTsId(new TimeSeriesIdentifierDescriptor.Builder()
                        .withTimeSeriesId("AUGU.Precip-Inc.Total.1Hour.1Hour.Ccp-Rev")
                        .withOfficeId("SWT")
                        .withActive(true)
                        .withIntervalOffsetMinutes(0L)
                        .withZoneId(ZoneId.of("UTC"))
                        .build())
                .withNotes("Precipitation data for AUGU")
                .withDateRefreshed(Instant.parse("2025-03-03T12:00:00Z"))
                .build();

        TimeSeriesMetaData ts4Stage = new TimeSeriesMetaData.Builder()
                .withTsId(new TimeSeriesIdentifierDescriptor.Builder()
                        .withTimeSeriesId("AUGU.Stage.Inst.1Hour.0.Ccp-Rev")
                        .withOfficeId("SWT")
                        .withActive(true)
                        .withIntervalOffsetMinutes(0L)
                        .withZoneId(ZoneId.of("UTC"))
                        .build())
                .withNotes("Stage data for AUGU")
                .withDateRefreshed(Instant.parse("2025-03-03T12:00:00Z"))
                .build();

        TimeSeriesMetaData ts5Outflow = new TimeSeriesMetaData.Builder()
                .withTsId(new TimeSeriesIdentifierDescriptor.Builder()
                        .withTimeSeriesId("AUGU.Flow.Inst.1Hour.0.Ccp-Rev")
                        .withOfficeId("SWT")
                        .withActive(true)
                        .withIntervalOffsetMinutes(0L)
                        .withZoneId(ZoneId.of("UTC"))
                        .build())
                .withNotes("Flow data for AUGU")
                .withDateRefreshed(Instant.parse("2025-03-03T12:00:00Z"))
                .build();

        TimeSeriesMetaData ts6OutflowAlt = new TimeSeriesMetaData.Builder()
                .withTsId(new TimeSeriesIdentifierDescriptor.Builder()
                        .withTimeSeriesId("ALTU.Flow-Res Out.Ave.1Hour.1Hour.Rev-Regi-Flowgroup")
                        .withOfficeId("SWT")
                        .withActive(true)
                        .withIntervalOffsetMinutes(0L)
                        .withZoneId(ZoneId.of("UTC"))
                        .build())
                .withNotes("Outflow data for ALTU")
                .withDateRefreshed(Instant.parse("2025-03-03T12:00:00Z"))
                .build();

        TimeSeriesMetaData ts7FloodStorage = new TimeSeriesMetaData.Builder()
                .withTsId(new TimeSeriesIdentifierDescriptor.Builder()
                        .withTimeSeriesId("ALTU.Stor-Flood Pool.Inst.1Hour.0.Ccp-Rev")
                        .withOfficeId("SWT")
                        .withActive(true)
                        .withIntervalOffsetMinutes(0L)
                        .withZoneId(ZoneId.of("UTC"))
                        .build())
                .withNotes("Flood storage data for ALTU")
                .withDateRefreshed(Instant.parse("2025-03-03T12:00:00Z"))
                .build();

        TimeSeriesMetaData ts8ConservationStorage = new TimeSeriesMetaData.Builder()
                .withTsId(new TimeSeriesIdentifierDescriptor.Builder()
                        .withTimeSeriesId("ALTU.Stor-Conservation Pool.Inst.1Hour.0.Ccp-Rev")
                        .withOfficeId("SWT")
                        .withActive(true)
                        .withIntervalOffsetMinutes(0L)
                        .withZoneId(ZoneId.of("UTC"))
                        .build())
                .withNotes("Conservation storage data for ALTU")
                .withDateRefreshed(Instant.parse("2025-03-03T12:00:00Z"))
                .build();

        // Expected data for locations
        TimeSeriesIdentifiersByType expectedAARK = new TimeSeriesIdentifiersByType.Builder()
                .withLocationId(new CwmsId.Builder()
                        .withName("AARK")
                        .withOfficeId("SWT")
                        .withBoundingOfficeId("SWT")
                        .withKind("SITE")
                        .build())
                .withTimeSeriesId("STAGE", ts1Stage)
                .withTimeSeriesId("OUTFLOW", ts2Outflow)
                .build();

        TimeSeriesIdentifiersByType expectedAUGU = new TimeSeriesIdentifiersByType.Builder()
                .withLocationId(new CwmsId.Builder()
                        .withName("AUGU")
                        .withOfficeId("SWT")
                        .withBoundingOfficeId("SWT")
                        .withKind("SITE")
                        .build())
                .withTimeSeriesId("PRECIP", ts3Precip)
                .withTimeSeriesId("STAGE", ts4Stage)
                .withTimeSeriesId("OUTFLOW", ts5Outflow)
                .build();

        TimeSeriesIdentifiersByType expectedALTU = new TimeSeriesIdentifiersByType.Builder()
                .withLocationId(new CwmsId.Builder()
                        .withName("ALTU")
                        .withOfficeId("SWT")
                        .withBoundingOfficeId("SWT")
                        .withKind("SITE")
                        .build())
                .withTimeSeriesId("OUTFLOW", ts6OutflowAlt)
                .withTimeSeriesId("FLOOD STORAGE", ts7FloodStorage)
                .withTimeSeriesId("CONSERVATION STORAGE", ts8ConservationStorage)
                .build();

        TimeSeriesIdentifiersByTypeList list = new TimeSeriesIdentifiersByTypeList.Builder()
                .withCursor("cursor")
                .withPageSize(20)
                .withTotal(100)
                .withTimeSeriesIdsForLocations(Arrays.asList(expectedAARK, expectedAUGU, expectedALTU))
                .build();

        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/time_series_identifiers_by_type.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);

        ContentType contentType = new ContentType(Formats.JSON);
        TimeSeriesIdentifiersByTypeList deserialized = Formats.parseContent(contentType, json, TimeSeriesIdentifiersByTypeList.class);

        // Verify the deserialized data matches the expected data
        assertAll(() -> DTOMatch.assertMatch(list, deserialized));
    }

}
