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

package cwms.cda.data.dao;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import cwms.cda.formatters.FormattingException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

final class RatingDaoTest {

    @ParameterizedTest(name = "{index}: extracts office-id \"{1}\"")
    @CsvSource(
        value = {
            "'<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<RatingSpec>\n" +
                "    <office-id>SWT</office-id>\n" +
                "    <rating-id>TENK.%-Opening,Elev;Flow.Standard.Production</rating-id>\n" +
                "    <template-id>%-Opening,Elev;Flow.Standard</template-id>\n" +
                "    <location-id>TENK</location-id>\n" +
                "    <version>Production</version>\n" +
                "    <in-range-method>LINEAR</in-range-method>\n" +
                "    <out-range-low-method>NEAREST</out-range-low-method>\n" +
                "    <out-range-high-method>NEAREST</out-range-high-method>\n" +
                "    <active>true</active>\n" +
                "    <auto-update>false</auto-update>\n" +
                "    <auto-activate>false</auto-activate>\n" +
                "    <auto-migrate-extension>false</auto-migrate-extension>\n" +
                "    <independent-rounding-specs>\n" +
                "        <position>0</position>\n" +
                "        <value>2222233332</value>\n" +
                "    </independent-rounding-specs>\n" +
                "    <dependent-rounding-spec>2222233332</dependent-rounding-spec>\n" +
                "    <effective-dates>2021-06-15T20:03:00Z</effective-dates>\n" +
                "</RatingSpec>'",
            "'<rating-template office-id=\"SWT\">\n" +
                "        <parameters-id>Stage;Stage-Corrected</parameters-id>\n" +
                "        <version>Linear</version>\n" +
                "        <ind-parameter-specs>\n" +
                "            <ind-parameter-spec position=\"1\">\n" +
                "                <parameter>Stage</parameter>\n" +
                "                <in-range-method>LINEAR</in-range-method>\n" +
                "                <out-range-low-method>NULL</out-range-low-method>\n" +
                "                <out-range-high-method>NULL</out-range-high-method>\n" +
                "            </ind-parameter-spec>\n" +
                "        </ind-parameter-specs>\n" +
                "        <dep-parameter>Stage-Corrected</dep-parameter>\n" +
                "        <description>Stream Stage Correction Rating</description>\n" +
                "    </rating-template>'"
        }
    )
    void extractOfficeFromXml_extractsOfficeId(String xml) {
        assertEquals("SWT", RatingDao.extractOfficeFromXml(xml));
    }

    @Test
    void extractOfficeFromXml_throwsIfMissing() {
        String xml = "<?xml version=\"1.0\"?><RatingSpec></RatingSpec>";
        assertThrows(FormattingException.class, () -> RatingDao.extractOfficeFromXml(xml));
    }
}
