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

package cwms.cda.data.dto.location.kind;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import cwms.cda.api.errors.FieldException;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

final class TurbineChangeTest {

    @ParameterizedTest
    @CsvSource({Formats.JSON, Formats.JSONV1, Formats.DEFAULT})
    void testTurbineSerializationRoundTrip(String format) {
        TurbineChange turbine = buildTestChange(Instant.now(), true);
        ContentType contentType = Formats.parseHeader(format, TurbineChange.class);
        String serialized = Formats.format(contentType, turbine);
        TurbineChange deserialized = Formats.parseContent(contentType, serialized, TurbineChange.class);
        DTOMatch.assertMatch(turbine, deserialized);
    }

    @ParameterizedTest
    @CsvSource({Formats.JSON, Formats.JSONV1, Formats.DEFAULT})
    void testTurbineSerializationFromJson(String format) throws Exception {
        ZonedDateTime first = ZonedDateTime.of(2024, 3, 4, 0, 0, 0, 0, ZoneId.of("America/Los_Angeles"));
        ZonedDateTime second = ZonedDateTime.of(2024, 3, 4, 1, 0, 0, 0, ZoneId.of("America/Los_Angeles"));
        ZonedDateTime third = ZonedDateTime.of(2024, 3, 4, 2, 0, 0, 0, ZoneId.of("America/Los_Angeles"));
        TurbineChange turbineChange1 = buildTestChange(first.toInstant(), true);
        TurbineChange turbineChange2 = buildTestChange(second.toInstant(), false);
        TurbineChange turbineChange3 = buildTestChange(third.toInstant(), true);
        InputStream resource =
            this.getClass().getResourceAsStream("/cwms/cda/data/dto/location/kind/turbine-settings.json");
        String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = Formats.parseHeader(format, TurbineChange.class);
        List<TurbineChange> deserialized = Formats.parseContentList(contentType, serialized, TurbineChange.class);
        DTOMatch.assertMatch(turbineChange1, deserialized.get(0));
        DTOMatch.assertMatch(turbineChange2, deserialized.get(1));
        DTOMatch.assertMatch(turbineChange3, deserialized.get(2));
    }

    @Test
    void testValidate() {
        assertAll(() -> {
                TurbineChange turbineChange = new TurbineChange.Builder()
                    .build();
                assertThrows(FieldException.class, turbineChange::validate,
                    "Expected validate() to throw FieldException because project-id field can't be null, but it didn't");
            }, () -> {
                TurbineChange turbineChange = new TurbineChange.Builder()
                    .withProjectId(new CwmsId.Builder()
                        .withName("PROJECT")
                        .build())
                    .withChangeDate(Instant.now())
                    .build();
                assertThrows(FieldException.class, turbineChange::validate,
                    "Expected validate() to throw FieldException because project-id must contain an office, but it didn't");
            }, () -> {
                TurbineChange turbineChange = new TurbineChange.Builder()
                    .withProjectId(new CwmsId.Builder()
                        .withName("PROJECT")
                        .withOfficeId("SPK")
                        .build())
                    .withChangeDate(Instant.now())
                    .build();
                assertThrows(FieldException.class, turbineChange::validate,
                    "Expected validate() to throw FieldException because discharge-computation-type field can't be null, but it didn't");
            }, () -> {
                TurbineChange turbineChange = new TurbineChange.Builder()
                    .withChangeDate(Instant.now())
                    .withDischargeComputationType(new LookupType.Builder()
                        .withOfficeId("SPK")
                        .withDisplayValue("R")
                        .build()).build();
                assertThrows(FieldException.class, turbineChange::validate,
                    "Expected validate() to throw FieldException because reason-type field can't be null, but it didn't");
            }
        );
    }

    private static TurbineChange buildTestChange(Instant changeDate, boolean isProtected) {
        Set<TurbineSetting> settings = new HashSet<>();
        settings.add(new TurbineSetting.Builder()
            .withLocationId(new CwmsId.Builder()
                .withOfficeId("SPK")
                .withName("PROJECT-TURBINE1")
                .build())
            .withDischargeUnits("cms")
            .withNewDischarge(164.004096)
            .withOldDischarge(164.004096)
            .withRealPower(35.1)
            .withScheduledLoad(35.0)
            .withGenerationUnits("MW")
            .build());
        settings.add(new TurbineSetting.Builder()
            .withLocationId(new CwmsId.Builder()
                .withOfficeId("SPK")
                .withName("PROJECT-TURBINE2")
                .build())
            .withDischargeUnits("cms")
            .withNewDischarge(163.478252)
            .withOldDischarge(163.478252)
            .withRealPower(35.5)
            .withScheduledLoad(35.0)
            .withGenerationUnits("MW")
            .build());
        settings.add(new TurbineSetting.Builder()
            .withLocationId(new CwmsId.Builder()
                .withOfficeId("SPK")
                .withName("TURBINE3")
                .build())
            .withDischargeUnits("cms")
            .withNewDischarge(163.478252)
            .withOldDischarge(163.478252)
            .withRealPower(35.5)
            .withScheduledLoad(35.0)
            .withGenerationUnits("MW")
            .build());
        return new TurbineChange.Builder()
            .withChangeDate(changeDate)
            .withDischargeUnits("cms")
            .withElevationUnits("m")
            .withProtected(isProtected)
            .withNotes("from SCADA")
            .withDischargeComputationType(new LookupType.Builder()
                .withActive(true)
                .withOfficeId("SPK")
                .withDisplayValue("R")
                .withTooltip("Reported by powerhouse")
                .build())
            .withPoolElevation(221.62008)
            .withTailwaterElevation(195.352416)
            .withProjectId(new CwmsId.Builder()
                .withOfficeId("SPK")
                .withName("PROJECT")
                .build())
            .withReasonType(new LookupType.Builder()
                .withActive(true)
                .withDisplayValue("S")
                .withTooltip("Scheduled release to meet loads")
                .withOfficeId("SPK")
                .build())
            .withNewTotalDischargeOverride(161.406026)
            .withOldTotalDischargeOverride(158.574341)
            .withSettings(settings)
            .build();
    }

}
