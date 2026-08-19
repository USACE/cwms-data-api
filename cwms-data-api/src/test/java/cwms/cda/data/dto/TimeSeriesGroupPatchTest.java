/*
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cwms.cda.data.dto.timeseriesgroup.Membership;
import cwms.cda.data.dto.timeseriesgroup.TimeSeriesGroupPatch;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import cwms.cda.helpers.DTOMatch;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

final class TimeSeriesGroupPatchTest {

    private static final String OFFICE_ID = "SPK";
    private static final String GROUP_ID = "group-id";
    private static final String CATEGORY_ID = "category-id";
    private static final String CATEGORY_DESCRIPTION = "category description";
    private static final String GROUP_DESCRIPTION = "patch description";
    private static final String SHARED_ALIAS_ID = "shared-alias";
    private static final String SHARED_REF_TS_ID = "Shared.Flow.Inst.1Hour.0.Raw";
    private static final String ASSIGN_TS_ID = "Loc.Flow.Inst.1Hour.0.Raw";
    private static final String ASSIGN_ALIAS_ID = "AliasId1";
    private static final String ASSIGN_REF_TS_ID = "Loc2.Flow.Inst.1Hour.0.Raw";
    private static final int ASSIGN_ATTRIBUTE = 5;
    private static final String UNASSIGN_TS_ID = "Loc3.Flow.Inst.1Hour.0.Raw";

    @Test
    void test_serialize_json() {
        TimeSeriesGroupPatch patch = buildTimeSeriesGroupPatch();

        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesGroupPatch.class);
        String result = Formats.format(contentType, patch);
        assertNotNull(result);

        assertTrue(result.contains("\"office-id\":\"" + OFFICE_ID + "\""));
        assertTrue(result.contains("\"id\":\"" + GROUP_ID + "\""));
        assertTrue(result.contains("\"time-series-category\""));
        assertTrue(result.contains("\"description\":\"" + GROUP_DESCRIPTION + "\""));
        assertTrue(result.contains("\"shared-alias-id\":\"" + SHARED_ALIAS_ID + "\""));
        assertTrue(result.contains("\"shared-ref-ts-id\":\"" + SHARED_REF_TS_ID + "\""));
        assertTrue(result.contains("\"membership\""));
        assertTrue(result.contains("\"assign\""));
        assertTrue(result.contains("\"unassign\""));
        assertTrue(result.contains(ASSIGN_TS_ID));
        assertTrue(result.contains(ASSIGN_ALIAS_ID));
        assertTrue(result.contains(UNASSIGN_TS_ID));
    }

    @Test
    void test_serialize_deserialize_roundtrip() {
        TimeSeriesGroupPatch patch = buildTimeSeriesGroupPatch();

        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesGroupPatch.class);
        String json = Formats.format(contentType, patch);
        TimeSeriesGroupPatch deserialized = Formats.parseContent(contentType, json, TimeSeriesGroupPatch.class);

        DTOMatch.assertMatch(patch, deserialized);
    }

    @Test
    void test_deserialize_from_file() throws IOException {
        String json;
        try (InputStream stream = getClass().getResourceAsStream("time_series_group_patch.json")) {
            assertNotNull(stream);
            json = IOUtils.toString(stream, StandardCharsets.UTF_8);
        }

        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesGroupPatch.class);
        TimeSeriesGroupPatch deserialized = Formats.parseContent(contentType, json, TimeSeriesGroupPatch.class);

        DTOMatch.assertMatch(buildTimeSeriesGroupPatch(), deserialized);
    }

    private TimeSeriesGroupPatch buildTimeSeriesGroupPatch() {
        TimeSeriesCategory category = new TimeSeriesCategory(OFFICE_ID, CATEGORY_ID, CATEGORY_DESCRIPTION);
        AssignedTimeSeries assign = new AssignedTimeSeries(OFFICE_ID, ASSIGN_TS_ID, ASSIGN_ALIAS_ID,
                ASSIGN_REF_TS_ID, ASSIGN_ATTRIBUTE);
        Membership membership = new Membership.Builder()
                .withAssign(Collections.singletonList(assign))
                .withUnassign(Collections.singletonList(UNASSIGN_TS_ID))
                .build();
        return new TimeSeriesGroupPatch.Builder()
                .withTimeSeriesCategory(category)
                .withOfficeId(OFFICE_ID)
                .withId(GROUP_ID)
                .withDescription(GROUP_DESCRIPTION)
                .withSharedAliasId(SHARED_ALIAS_ID)
                .withSharedRefTsId(SHARED_REF_TS_ID)
                .withMembership(membership)
                .build();
    }


}
