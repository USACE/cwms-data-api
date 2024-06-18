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

package cwms.cda.data.dto.project;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import cwms.cda.data.dto.Location;
import cwms.cda.formatters.json.JsonV2;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

class ProjectTest {


    @Test
    void testProject() throws JsonProcessingException {

        Location pbLoc = new Location.Builder("SPK","Pumpback Location Id")
                .withTimeZoneName(ZoneId.of("UTC"))
                .withActive(null)
                .build();
        Location ngLoc = new Location.Builder("SPK","Near Gage Location Id")
                .withTimeZoneName(ZoneId.of("UTC"))
                .withActive(null)
                .build();

        Location prjLoc = new Location.Builder("SPK","Project Location Id")
                .withTimeZoneName(ZoneId.of("UTC"))
                .withActive(null)
                .build();

        Project project = new Project.Builder()
                .withLocation(prjLoc)
                .withProjectOwner("Project Owner")
                .withAuthorizingLaw("Authorizing Law")
                .withFederalCost(100.0)
                .withNonFederalCost(50.0)
                .withFederalOAndMCost(10.0)
                .withNonFederalOAndMCost(5.0)
                .withCostYear(Instant.now())
                .withCostUnit("$")
                .withYieldTimeFrameEnd(Instant.now())
                .withYieldTimeFrameStart(Instant.now())
                .withFederalOAndMCost(10.0)
                .withNonFederalOAndMCost(5.0)
                .withProjectRemarks("Remarks")
                .withPumpBackLocation(pbLoc)
                .withNearGageLocation(ngLoc)
                .withBankFullCapacityDesc("Bank Full Capacity Description")
                .withDownstreamUrbanDesc("Downstream Urban Description")
                .withHydropowerDesc("Hydropower Description")
                .withSedimentationDesc("Sedimentation Description")
                .build();

        ObjectMapper om = JsonV2.buildObjectMapper();
        ObjectWriter ow = om.writerWithDefaultPrettyPrinter();

        String json = ow.writeValueAsString(project);
        assertNotNull(json);
    }

    @Test
    void testDeserialize() throws IOException {
        InputStream stream = ProjectTest.class.getClassLoader().getResourceAsStream(
                "cwms/cda/data/dto/project.json");
        assertNotNull(stream);
        String input = IOUtils.toString(stream, StandardCharsets.UTF_8);

        ObjectMapper om = JsonV2.buildObjectMapper();
        Project project = om.readValue(input, Project.class);

        assertNotNull(project);

        Location loc = project.getLocation();
        assertNotNull(loc);
        assertEquals("SPK", loc.getOfficeId());
        assertEquals("Project Id", loc.getName());
        assertEquals("Project Owner", project.getProjectOwner());
        assertEquals("Authorizing Law", project.getAuthorizingLaw());
        assertEquals(100.0, project.getFederalCost());
        assertEquals(50.0, project.getNonFederalCost());
        assertEquals(10.0, project.getFederalOAndMCost());
        assertEquals(5.0, project.getNonFederalOAndMCost());
        assertEquals(1717199914902L, project.getCostYear().toEpochMilli());
        assertEquals("$", project.getCostUnit());
        assertEquals(1717199914902L, project.getYieldTimeFrameStart().toEpochMilli());
        assertEquals(1717199914902L, project.getYieldTimeFrameEnd().toEpochMilli());
        assertEquals("Remarks", project.getProjectRemarks());
        assertEquals("Pumpback Location Id", project.getPumpBackLocation().getName());
        assertEquals("SPK", project.getPumpBackLocation().getOfficeId());
        assertEquals("Near Gage Location Id", project.getNearGageLocation().getName());
        assertEquals("SPK", project.getNearGageLocation().getOfficeId());

        assertEquals("Bank Full Capacity Description", project.getBankFullCapacityDesc());
        assertEquals("Downstream Urban Description", project.getDownstreamUrbanDesc());
        assertEquals("Hydropower Description", project.getHydropowerDesc());
        assertEquals("Sedimentation Description", project.getSedimentationDesc());
    }
}
