package cwms.cda.data.dto.rating;

import static cwms.cda.data.dto.rating.RatingSpec.Builder.buildIndependentRoundingSpecs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.formatters.xml.XMLv2;
import org.junit.jupiter.api.Test;

public class RatingSpecTest {

    @Test
    void testSerialize() throws JsonProcessingException {
        String officeId = "SWT";
        String ratingId = "ARBU.Elev;Stor.Linear.Production";

        RatingSpec spec = buildRatingSpec(officeId, ratingId);

        ObjectMapper om = JsonV2.buildObjectMapper();
        String serializedLocation = om.writeValueAsString(spec);
        assertNotNull(serializedLocation);

    }

    @Test
    void testRoundtripJSON() throws JsonProcessingException {
        String officeId = "SWT";
        String ratingId = "ARBU.Elev;Stor.Linear.Production";

        RatingSpec spec = buildRatingSpec(officeId, ratingId);

        ObjectMapper om = JsonV2.buildObjectMapper();
        String serializedSpec = om.writeValueAsString(spec);
        assertNotNull(serializedSpec);

        RatingSpec spec2 = om.readValue(serializedSpec, RatingSpec.class);
        assertNotNull(spec2);
        assertEquals(spec, spec2);
    }

    @Test
    void testRoundtripXML() {
        String officeId = "SWT";
        String ratingId = "ARBU.Elev;Stor.Linear.Production";

        RatingSpec spec = buildRatingSpec(officeId, ratingId);

        XMLv2 xmlv2 = new XMLv2();

        String xml = xmlv2.format(spec);
        assertNotNull(xml);
        System.out.println(xml);
        assertTrue(xml.contains("ARBU.Elev;Stor.Linear.Production"));

        RatingSpec spec2 = xmlv2.parseContent(xml, RatingSpec.class);
        assertNotNull(spec2);
        assertEquals(spec, spec2);
    }

    public static RatingSpec buildRatingSpec(String officeId, String ratingId) {
        RatingSpec retval;

        String templateId = "Elev;Stor.Linear";
        String locId = "ARBU";
        String version = "Production";
        String agency = null;

        boolean activeFlag = true;

        boolean autoUpdateFlag = false;

        boolean autoActivateFlag = false;

        boolean autoMigrateExtFlag = false;
        String indRndSpecs = "2222233332";

        String depRndSpecs = "2222233332";
        String desc = null;

        String dateMethods = "LINEAR,NEAREST,LOWER";

        RatingSpec.Builder builder = new RatingSpec.Builder();
        builder = builder
                .withOfficeId(officeId).withRatingId(ratingId)
                .withTemplateId(templateId).withLocationId(locId)
                .withVersion(version).withSourceAgency(agency)
                .withActive(activeFlag).withAutoUpdate(autoUpdateFlag)
                .withAutoActivate(autoActivateFlag)
                .withAutoMigrateExtension(autoMigrateExtFlag)
                .withIndependentRoundingSpecs(buildIndependentRoundingSpecs(indRndSpecs))
                .withDependentRoundingSpec(depRndSpecs).withDescription(desc)
                .withDateMethods(dateMethods);
        retval = builder.build();

        assertEquals("LINEAR", retval.getOutRangeLowMethod());
        assertEquals("NEAREST", retval.getInRangeMethod());
        assertEquals("LOWER", retval.getOutRangeHighMethod());

        RatingSpec testSpec = builder.withInRangeMethod("InRange")
                .withOutRangeLowMethod("OutRangeLow")
                .withOutRangeHighMethod("OutRangeHigh")
                .build();

        assertEquals("OutRangeLow", testSpec.getOutRangeLowMethod());
        assertEquals("InRange", testSpec.getInRangeMethod());
        assertEquals("OutRangeHigh", testSpec.getOutRangeHighMethod());


        return retval;
    }

}