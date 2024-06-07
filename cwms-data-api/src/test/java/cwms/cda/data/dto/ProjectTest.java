package cwms.cda.data.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import cwms.cda.formatters.json.JsonV2;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

class ProjectTest {


    @Test
    void testProject() throws JsonProcessingException {

        Location pbLoc = new Location.Builder("SPK","Pumpback Location Id")
                .withActive(null)
                .build();
        Location ngLoc = new Location.Builder("SPK","Near Gage Location Id")
                .withActive(null)
                .build();

        Project project = new Project.Builder()
                .withOfficeId("SPK")
                .withName("Project Id")
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

        assertEquals("SPK", project.getOfficeId());
        assertEquals("Project Id", project.getName());
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
