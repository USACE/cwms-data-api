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

package cwms.cda.data.dao.location.kind;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.location.kind.TurbineChange;
import cwms.cda.data.dto.location.kind.TurbineSetting;
import cwms.cda.helpers.DTOMatch;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

final class TurbineDaoTest extends DataApiTestIT {

    @Test
    void testJooqMapping() {
        TurbineChange turbineChange = buildTestChange();
        TurbineChange unmapped = TurbineDao.map(TurbineDao.map(turbineChange));
        DTOMatch.assertMatch(turbineChange, unmapped);
    }

    private static TurbineChange buildTestChange() {
        List<TurbineSetting> settings = new ArrayList<>();
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
            .withChangeDate(Instant.now())
            .withDischargeUnits("cms")
            .withElevationUnits("m")
            .withProtected(true)
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
