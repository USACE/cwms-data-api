/*
 * MIT License
 * Copyright (c) 2024 Hydrologic Engineering Center
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto.location.kind;

import cwms.cda.api.errors.RequiredFieldException;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.LookupType;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DTOMatch;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class GateChangeTest {
    private static final String OFFICE_ID = "SPK";
    private static final String PROJECT_LOC = "BIGH";
    private static final CwmsId PROJECT_ID = new CwmsId.Builder().withOfficeId(OFFICE_ID).withName(PROJECT_LOC).build();
    private static final CwmsId BIGH_TG_1 = new CwmsId.Builder().withOfficeId(OFFICE_ID)
                                                                .withName(PROJECT_LOC + "-TG1")
                                                                .build();
    private static final CwmsId BIGH_TG_2 = new CwmsId.Builder().withOfficeId(OFFICE_ID)
                                                                .withName("TG2")
                                                                .build();
    private static final Instant JAN_FIRST = ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneId.of("America/Los_Angeles"))
                                                          .toInstant();
    private static final Instant JAN_SECOND = ZonedDateTime.of(2024, 1, 2, 0, 0, 0, 0, ZoneId.of("America/Los_Angeles"))
                                                           .toInstant();
    private static final GateChange TEST_GATE_CHANGE = buildTestGateChange(PROJECT_ID, JAN_FIRST,
                                                                           GateSettingTest.buildTestGateSetting(
                                                                                   BIGH_TG_1, 0, 1),
                                                                           GateSettingTest.buildTestGateSetting(
                                                                                   BIGH_TG_2, 0, 1));
    private static final GateChange SECOND_GATE_CHANGE = buildTestGateChange(PROJECT_ID, JAN_SECOND,
                                                                             GateSettingTest.buildTestGateSetting(
                                                                                     BIGH_TG_1, 1, 2),
                                                                             GateSettingTest.buildTestGateSetting(
                                                                                     BIGH_TG_2, 1, 2));

    @Test
    void testSingleSerialization() {
        ContentType contentType = Formats.parseHeader(Formats.JSON, GateChange.class);
        String json = Formats.format(contentType, TEST_GATE_CHANGE);

        GateChange parsedOutlet = Formats.parseContent(contentType, json, GateChange.class);
        DTOMatch.assertMatch(TEST_GATE_CHANGE, parsedOutlet);
    }

    @Test
    void testMultiSerialization() {
        ContentType contentType = Formats.parseHeader(Formats.JSON, GateChange.class);
        List<GateChange> changes = Arrays.asList(TEST_GATE_CHANGE, SECOND_GATE_CHANGE);
        String json = Formats.format(contentType, changes, GateChange.class);

        List<GateChange> parsedOutlet = Formats.parseContentList(contentType, json, GateChange.class);
        DTOMatch.assertMatch(changes, parsedOutlet, DTOMatch::assertMatch);
    }

    @Test
    void testSingleFileSerialization() throws Exception {
        ContentType contentType = Formats.parseHeader(Formats.JSON, GateChange.class);
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/location/kind/gate-change.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        GateChange deserialized = assertDoesNotThrow(() -> Formats.parseContent(contentType, json, GateChange.class), "Unable to parse gate change json");
        DTOMatch.assertMatch(TEST_GATE_CHANGE, deserialized);
    }

    @Test
    void testMultiFileSerialization() throws Exception {
        ContentType contentType = Formats.parseHeader(Formats.JSON, GateChange.class);
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/location/kind/gate-changes.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        List<GateChange> deserialized = assertDoesNotThrow(() -> Formats.parseContentList(contentType, json, GateChange.class), "Unable to parse gate change json");
        List<GateChange> changes = Arrays.asList(TEST_GATE_CHANGE, SECOND_GATE_CHANGE);
        DTOMatch.assertMatch(changes, deserialized, DTOMatch::assertMatch);
    }

    @Test
    void testMissingRequiredFields() {
        LookupType dischargeComputationType = new LookupType.Builder().withActive(true)
                                                                      .withDisplayValue("A")
                                                                      .withTooltip("Adjusted by an automated method")
                                                                      .withOfficeId("CWMS")
                                                                      .build();
        LookupType reasonType = new LookupType.Builder().withActive(true)
                                                        .withDisplayValue("O")
                                                        .withTooltip("Other release")
                                                        .withOfficeId("CWMS")
                                                        .build();
        boolean isProtected = true;
        Double newTotalDischargeOverride = 1.0;
        Double oldTotalDischargeOverride = 2.0;
        String dischargeUnits = "cfs";
        Double poolElevation = 3.0;
        Double tailwaterElevation = 4.0;
        String elevationUnits = "ft";
        String notes = "Test notes";

        GateChange.Builder builder = new GateChange.Builder().withProjectId(PROJECT_ID)
                                                             .withDischargeComputationType(dischargeComputationType)
                                                             .withReasonType(reasonType)
                                                             .withProtected(isProtected)
                                                             .withNewTotalDischargeOverride(newTotalDischargeOverride)
                                                             .withOldTotalDischargeOverride(oldTotalDischargeOverride)
                                                             .withDischargeUnits(dischargeUnits)
                                                             .withPoolElevation(poolElevation)
                                                             .withTailwaterElevation(tailwaterElevation)
                                                             .withElevationUnits(elevationUnits)
                                                             .withNotes(notes)
                                                             .withChangeDate(JAN_FIRST)
                                                             .withSettings(new ArrayList<>());
        assertAll(() -> assertDoesNotThrow(() -> builder.build().validate()),
                  () -> assertThrows(RequiredFieldException.class, () -> builder.withProjectId(null)
                                                                                .build()
                                                                                .validate(),
                                     "project-id"),
                  () -> assertThrows(RequiredFieldException.class, () -> builder.withProjectId(PROJECT_ID)
                                                                                .withDischargeComputationType(null)
                                                                                .build()
                                                                                .validate(),
                                     "discharge-computation-type"),
                  () -> assertThrows(RequiredFieldException.class, () -> builder.withDischargeComputationType(dischargeComputationType)
                                                                                .withReasonType(null)
                                                                                .build()
                                                                                .validate(),
                                     "reason-type"),
                  () -> assertThrows(RequiredFieldException.class, () -> builder.withReasonType(reasonType)
                                                                                .withPoolElevation(null)
                                                                                .build()
                                                                                .validate(),
                                     "pool-elevation"),
                  () -> assertThrows(RequiredFieldException.class, () -> builder.withPoolElevation(poolElevation)
                                                                                .withChangeDate(null)
                                                                                .build()
                                                                                .validate(),
                                     "change-date")
        );
    }

    static GateChange buildTestGateChange(CwmsId projectId, Instant changeDate, GateSetting ... settingVargs) {
        LookupType dischargeComputationType = new LookupType.Builder().withActive(true)
                                                                      .withDisplayValue("A")
                                                                      .withTooltip("Adjusted by an automated method")
                                                                      .withOfficeId("CWMS")
                                                                      .build();
        LookupType reasonType = new LookupType.Builder().withActive(true)
                                                        .withDisplayValue("O")
                                                        .withTooltip("Other release")
                                                        .withOfficeId("CWMS")
                                                        .build();
        boolean isProtected = true;
        Double newTotalDischargeOverride = 1.0;
        Double oldTotalDischargeOverride = 2.0;
        String dischargeUnits = "cfs";
        Double poolElevation = 3.0;
        Double tailwaterElevation = 4.0;
        String elevationUnits = "ft";
        String notes = "Test notes";
        List<GateSetting> settings = Arrays.asList(settingVargs);

        return new GateChange.Builder().withProjectId(projectId)
                                       .withDischargeComputationType(dischargeComputationType)
                                       .withReasonType(reasonType)
                                       .withProtected(isProtected)
                                       .withNewTotalDischargeOverride(newTotalDischargeOverride)
                                       .withOldTotalDischargeOverride(oldTotalDischargeOverride)
                                       .withDischargeUnits(dischargeUnits)
                                       .withPoolElevation(poolElevation)
                                       .withTailwaterElevation(tailwaterElevation)
                                       .withElevationUnits(elevationUnits)
                                       .withNotes(notes)
                                       .withChangeDate(changeDate)
                                       .withSettings(settings)
                                       .build();
    }
}