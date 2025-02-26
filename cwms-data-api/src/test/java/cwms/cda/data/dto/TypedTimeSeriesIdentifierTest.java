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

final class TypedTimeSeriesIdentifierTest {

    @Test
    void createAccessToWaterTimeSeriesData_allFieldsProvided_success() {
        TimeSeriesIdentifierDescriptor tsDescriptor = new TimeSeriesIdentifierDescriptor.Builder()
                .withTimeSeriesId("VANL.Stage.Inst.15Minutes.0.Ccp-Rev")
                .withOfficeId("SWT")
                .withActive(true)
                .withIntervalOffsetMinutes(0L)
                .withZoneId(ZoneId.of("UTC"))
                .build();
        TimeSeriesIdentifierDescriptor tsDescriptor2 = new TimeSeriesIdentifierDescriptor.Builder()
                .withTimeSeriesId("VANL.Flow.Inst.15Minutes.0.Ccp-Rev")
                .withOfficeId("SWT")
                .withActive(true)
                .withIntervalOffsetMinutes(0L)
                .withZoneId(ZoneId.of("UTC"))
                .build();
        TypedTimeSeriesIdentifiers items = new TypedTimeSeriesIdentifiers.Builder()
                .withLocationId(CwmsId.buildCwmsId("SWT", "VANL"))
                .withTypeToTsId("STAGE", tsDescriptor)
                .withTypeToTsId("OUTFLOW", tsDescriptor2)
                .build();

        assertAll(
                () -> DTOMatch.assertMatch(CwmsId.buildCwmsId("SWT", "VANL"), items.getLocationId(), "Location ID"),
                () -> DTOMatch.assertMatch(tsDescriptor, items.getTypeToTsIdMap().get("STAGE")),
                () -> DTOMatch.assertMatch(tsDescriptor2, items.getTypeToTsIdMap().get("OUTFLOW"))
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
        Map<String, TimeSeriesIdentifierDescriptor> typeToTsIdMap = new HashMap<>();
        typeToTsIdMap.put("STAGE", tsDescriptor);
        assertAll(
                () -> assertThrows(FieldException.class, () -> {
                    TypedTimeSeriesIdentifiers item = new TypedTimeSeriesIdentifiers.Builder()
                            .withTypeToTsIdMap(typeToTsIdMap)
                            .build();
                    item.validate();
                }, "The validate method should have thrown a FieldException because the location ID is missing")
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
        TypedTimeSeriesIdentifiers data = new TypedTimeSeriesIdentifiers.Builder()
                .withLocationId(CwmsId.buildCwmsId("SWT", "VANL"))
                .withTypeToTsId("STAGE", tsDescriptor)
                .build();

        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, data);
        TypedTimeSeriesIdentifiers deserialized = Formats.parseContent(contentType, json, TypedTimeSeriesIdentifiers.class);
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
        TypedTimeSeriesIdentifiers expected = new TypedTimeSeriesIdentifiers.Builder()
                .withLocationId(CwmsId.buildCwmsId("SWT", "VANL"))
                .withTypeToTsId("STAGE", tsDescriptor)
                .build();

        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/typed_time_series_identifiers.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        TypedTimeSeriesIdentifiers deserialized = Formats.parseContent(contentType, json, TypedTimeSeriesIdentifiers.class);
        DTOMatch.assertMatch(expected, deserialized);
    }
}
