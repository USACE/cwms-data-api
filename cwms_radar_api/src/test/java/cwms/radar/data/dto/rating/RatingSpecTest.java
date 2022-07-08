package cwms.radar.data.dto.rating;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.radar.formatters.json.JsonV1;
import cwms.radar.formatters.json.JsonV2;
import org.junit.jupiter.api.Test;

import static cwms.radar.data.dto.rating.RatingSpec.Builder.buildIndependentRoundingSpecs;
import static org.junit.jupiter.api.Assertions.*;

public class RatingSpecTest
{

	@Test
	void testSerialize() throws JsonProcessingException
	{
		String officeId = "SWT";
		String ratingId = "ARBU.Elev;Stor.Linear.Production";

		RatingSpec spec = buildRatingSpec(officeId, ratingId);

		ObjectMapper om = JsonV2.buildObjectMapper();
		String serializedLocation = om.writeValueAsString(spec);
		assertNotNull(serializedLocation);

	}

	@Test
	void testRoundtripJSON() throws JsonProcessingException
	{
		String officeId = "SWT";
		String ratingId = "ARBU.Elev;Stor.Linear.Production";

		RatingSpec spec = buildRatingSpec(officeId, ratingId);

		ObjectMapper om = JsonV2.buildObjectMapper();
		String serializedLocation = om.writeValueAsString(spec);
		assertNotNull(serializedLocation);

		RatingSpec spec2 = om.readValue(serializedLocation, RatingSpec.class);
		assertNotNull(spec2);

	}

	public static RatingSpec buildRatingSpec(String officeId, String ratingId)
	{
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

		retval = new RatingSpec.Builder().withOfficeId(officeId).withRatingId(ratingId)
				.withTemplateId(templateId).withLocationId(locId).withVersion(version).withSourceAgency(agency)
				.withActive(activeFlag).withAutoUpdate(autoUpdateFlag).withAutoActivate(autoActivateFlag)
				.withAutoMigrateExtension(autoMigrateExtFlag)
				.withIndependentRoundingSpecs(buildIndependentRoundingSpecs(indRndSpecs))
				.withDependentRoundingSpec(depRndSpecs).withDescription(desc)
				.withDateMethods(dateMethods)
				.build();

		assertEquals("LINEAR", retval.getOutRangeLowMethod());
		assertEquals("NEAREST", retval.getInRangeMethod());
		assertEquals("LOWER", retval.getOutRangeHighMethod());

		return retval;
	}

}