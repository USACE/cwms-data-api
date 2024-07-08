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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import cwms.cda.data.dto.Location;
import cwms.cda.formatters.json.JsonV2;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ProjectTest {


    @Test
    void testProject() throws JsonProcessingException {

        Location pbLoc = new Location.Builder("SPK", "Pumpback Location Id")
                .withTimeZoneName(ZoneId.of("UTC"))
                .withActive(null)
                .build();
        Location ngLoc = new Location.Builder("SPK", "Near Gage Location Id")
                .withTimeZoneName(ZoneId.of("UTC"))
                .withActive(null)
                .build();

        Location prjLoc = new Location.Builder("SPK", "Project Location Id")
                .withTimeZoneName(ZoneId.of("UTC"))
                .withActive(null)
                .build();

        Project project = new Project.Builder()
                .withLocation(prjLoc)
                .withProjectOwner("Project Owner")
                .withAuthorizingLaw("Authorizing Law")
                .withFederalCost(BigDecimal.valueOf(100.0))
                .withNonFederalCost(BigDecimal.valueOf(50.0))
                .withFederalOAndMCost(BigDecimal.valueOf(10.0))
                .withNonFederalOAndMCost(BigDecimal.valueOf(5.0))
                .withCostYear(Instant.now())
                .withCostUnit("$")
                .withYieldTimeFrameEnd(Instant.now())
                .withYieldTimeFrameStart(Instant.now())
                .withFederalOAndMCost(BigDecimal.valueOf(10.0))
                .withNonFederalOAndMCost(BigDecimal.valueOf(5.0))
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


        assertAll(
                () -> assertEquals("SPK", loc.getOfficeId(), "Office ID does not match"),
                () -> assertEquals("Project Id", loc.getName(), "Project ID does not match"),
                () -> assertEquals("Project Owner", project.getProjectOwner(), "Project Owner does not match"),
                () -> assertEquals("Authorizing Law", project.getAuthorizingLaw(), "Authorizing Law does not match"),
                () -> assertEquals(100.0, project.getFederalCost().doubleValue(), "Federal Cost does not match"),
                () -> assertEquals(50.0, project.getNonFederalCost().doubleValue(), "Non-federal Cost does not match"),
                () -> assertEquals(10.0, project.getFederalOAndMCost().doubleValue(), "Federal O And M Cost does not match"),
                () -> assertEquals(5.0, project.getNonFederalOAndMCost().doubleValue(), "Non-Federal O And M Cost does not match"),
                () -> assertEquals(1717199914902L, project.getCostYear().toEpochMilli(), "Cost Year does not match"),
                () -> assertEquals("$", project.getCostUnit(), "Cost Unit does not match"),
                () -> assertEquals(1717199914902L, project.getYieldTimeFrameStart().toEpochMilli(), "Yield Time Frame Start does not match"),
                () -> assertEquals(1717199914902L, project.getYieldTimeFrameEnd().toEpochMilli(), "Yield Time Frame End does not match"),
                () -> assertEquals("Remarks", project.getProjectRemarks(), "Project Remarks do not match"),
                () -> assertEquals("Pumpback Location Id", project.getPumpBackLocation().getName(), "Pumpback Location ID does not match"),
                () -> assertEquals("SPK", project.getPumpBackLocation().getOfficeId(), "Pumpback Location Office ID does not match"),
                () -> assertEquals("Near Gage Location Id", project.getNearGageLocation().getName(), "Near Gage Location ID does not match"),
                () -> assertEquals("SPK", project.getNearGageLocation().getOfficeId(), "Near Gage Location Office ID does not match"),
                () -> assertEquals("Bank Full Capacity Description", project.getBankFullCapacityDesc(), "Bank Full Capacity Description does not match"),
                () -> assertEquals("Downstream Urban Description", project.getDownstreamUrbanDesc(), "Downstream Urban Description does not match"),
                () -> assertEquals("Hydropower Description", project.getHydropowerDesc(), "Hydropower Description does not match"),
                () -> assertEquals("Sedimentation Description", project.getSedimentationDesc(), "Sedimentation Description does not match")
        );

    }

    @Test
    void testRoundtrip() throws IOException {
        InputStream stream = ProjectTest.class.getClassLoader().getResourceAsStream(
                "cwms/cda/data/dto/project.json");
        assertNotNull(stream);
        String input = IOUtils.toString(stream, StandardCharsets.UTF_8);

        ObjectMapper om = JsonV2.buildObjectMapper();
        Project project = om.readValue(input, Project.class);
        assertNotNull(project);

        String json = om.writeValueAsString(project);
        Project project2 = om.readValue(json, Project.class);
        assertNotNull(project2);

        assertSame(project, project2);
    }

    public static void assertSame(Project project, Project project2) {

        assertAll(
                () -> assertEquals(project.getLocation().getOfficeId(), project2.getLocation().getOfficeId(), "Location Offices don't match"),
                () -> assertEquals(project.getLocation().getName(), project2.getLocation().getName(), "Location names don't match"),
                () -> assertEquals(project.getProjectOwner(), project2.getProjectOwner(), "Project Owners don't match"),
                () -> assertEquals(project.getAuthorizingLaw(), project2.getAuthorizingLaw(), "Authorizing Laws don't match"),
                () -> assertEquals(project.getFederalCost(), project2.getFederalCost(), "Federal Costs don't match"),
                () -> assertEquals(project.getNonFederalCost(), project2.getNonFederalCost(), "Non-Federal Costs don't match"),
                () -> assertEquals(project.getFederalOAndMCost(), project2.getFederalOAndMCost(), "Federal O And M Costs don't match"),
                () -> assertEquals(project.getNonFederalOAndMCost(), project2.getNonFederalOAndMCost(), "Non-Federal O And M Costs don't match"),
                () -> assertEquals(project.getCostYear(), project2.getCostYear(), "Cost Years don't match"),
                () -> assertEquals(project.getCostUnit(), project2.getCostUnit(), "Cost Units don't match"),
                () -> assertEquals(project.getYieldTimeFrameStart(), project2.getYieldTimeFrameStart(), "Yield TimeFrame Starts don't match"),
                () -> assertEquals(project.getYieldTimeFrameEnd(), project2.getYieldTimeFrameEnd(), "Yield TimeFrame Ends don't match"),
                () -> assertEquals(project.getProjectRemarks(), project2.getProjectRemarks(), "Project Remarks don't match"),
                () -> assertEquals(project.getPumpBackLocation().getName(), project2.getPumpBackLocation().getName(), "Pump Back Location Names don't match"),
                () -> assertEquals(project.getPumpBackLocation().getOfficeId(), project2.getPumpBackLocation().getOfficeId(), "Pump Back Location Offices don't match"),
                () -> assertEquals(project.getNearGageLocation().getName(), project2.getNearGageLocation().getName(), "Near Gage Location Names don't match"),
                () -> assertEquals(project.getNearGageLocation().getOfficeId(), project2.getNearGageLocation().getOfficeId(), "Near Gage Location Offices don't match"),
                () -> assertEquals(project.getBankFullCapacityDesc(), project2.getBankFullCapacityDesc(), "Bank Full Capacities don't match"),
                () -> assertEquals(project.getDownstreamUrbanDesc(), project2.getDownstreamUrbanDesc(), "Downstream Urban Descriptions don't match"),
                () -> assertEquals(project.getHydropowerDesc(), project2.getHydropowerDesc(), "Hydropower Descriptions don't match"),
                () -> assertEquals(project.getSedimentationDesc(), project2.getSedimentationDesc(), "Sedimentation Descriptions don't match")
        );
    }

}
