package cwms.cda.data.dto.project;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.data.dao.project.ProjectKind;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.json.JsonV2;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;


public class LocationsWithProjectKindTest {

    @Test
    void test_serialize() throws JsonProcessingException {
        LocationsWithProjectKind.Builder builder = new LocationsWithProjectKind.Builder();
        builder.withKind(ProjectKind.EMBANKMENT);
        List<CwmsId> locs = new ArrayList<>();
        locs.add(new CwmsId.Builder().withName("Emb1").withOfficeId("SPK").build());
        locs.add(new CwmsId.Builder().withName("Emb2").withOfficeId("SPK").build());
        builder.withLocations(locs);
        LocationsWithProjectKind locWithKind = builder.build();

        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        String json = objectMapper.writeValueAsString(locWithKind);
        assertNotNull(json);

    }

    @Test
    void test_deserialize() throws IOException {

        InputStream stream = LocationsWithProjectKind.class.getClassLoader().getResourceAsStream(
                "cwms/cda/data/dto/location_with_kind.json");
        assertNotNull(stream);
        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        LocationsWithProjectKind locationsWithKind = objectMapper.readValue(stream, LocationsWithProjectKind.class);
        assertNotNull(locationsWithKind);
        assertEquals(ProjectKind.EMBANKMENT, locationsWithKind.getKind());
        assertEquals("TestEmbankment1", locationsWithKind.getLocations().get(0).getName());
        assertEquals("TestEmbankment2", locationsWithKind.getLocations().get(1).getName());

    }

}