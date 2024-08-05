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

import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DTOMatch;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class GateSettingTest {
    private static final String OFFICE_ID = "SPK";
    private static final String PROJECT_LOC = "BIGH";
    private static final CwmsId BIGH_TG_1 = new CwmsId.Builder().withOfficeId(OFFICE_ID)
                                                                .withName(PROJECT_LOC + "-TG1")
                                                                .build();
    private static final CwmsId BIGH_TG_2 = new CwmsId.Builder().withOfficeId(OFFICE_ID)
                                                                .withName("TG2")
                                                                .build();
    private static final GateSetting TEST_GATE_SETTING = GateSettingTest.buildTestGateSetting(BIGH_TG_1, 0, 1);
    private static final GateSetting SECOND_GATE_SETTING = GateSettingTest.buildTestGateSetting(BIGH_TG_2, 2, 3);

    @Test
    void testSingleSerialization() {
        ContentType contentType = Formats.parseHeader(Formats.JSON, GateSetting.class);
        String json = Formats.format(contentType, TEST_GATE_SETTING);

        GateSetting parsedOutlet = Formats.parseContent(contentType, json, GateSetting.class);
        DTOMatch.assertMatch(TEST_GATE_SETTING, parsedOutlet);
    }

    @Test
    void testSingleFileSerialization() throws Exception {
        ContentType contentType = Formats.parseHeader(Formats.JSON, GateSetting.class);
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/location/kind/gate-setting.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        GateSetting deserialized = assertDoesNotThrow(() -> Formats.parseContent(contentType, json, GateSetting.class), "Unable to parse gate change json");
        DTOMatch.assertMatch(TEST_GATE_SETTING, deserialized);
    }

    @Test
    void testMultiFileSerialization() throws Exception {
        ContentType contentType = Formats.parseHeader(Formats.JSON, GateSetting.class);
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/location/kind/gate-settings.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        List<GateSetting> deserialized = assertDoesNotThrow(() -> Formats.parseContentList(contentType, json, GateSetting.class), "Unable to parse gate change json");
        List<GateSetting> changes = Arrays.asList(TEST_GATE_SETTING, SECOND_GATE_SETTING);
        DTOMatch.assertMatch(changes, deserialized, DTOMatch::assertMatch);
    }

    static GateSetting buildTestGateSetting(CwmsId locationId, double opening, double invertElev) {
        return new GateSetting.Builder().withLocationId(locationId)
                                        .withOpening(opening)
                                        .withOpeningParameter("Opening")
                                        .withOpeningUnits("ft")
                                        .withInvertElevation(invertElev)
                                        .build();
    }
}