package cwms.radar.data.dto.rating;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.radar.data.dto.TimeSeries;
import cwms.radar.formatters.json.JsonV1;
import org.junit.jupiter.api.Test;

import static cwms.radar.data.dto.rating.RatingSpec.Builder.buildIndRoundingSpecs;
import static org.junit.jupiter.api.Assertions.*;

public class RatingSpecTest
{

	@Test
	void testSerialize() throws JsonProcessingException
	{
		String officeId = "SWT";
		String ratingId = "ARBU.Elev;Stor.Linear.Production";

		RatingSpec spec = buildRatingSpec(officeId, ratingId);

		ObjectMapper om = new ObjectMapper();
		String serializedLocation = om.writeValueAsString(spec);
		assertNotNull(serializedLocation);

	}

	@Test
	void testRoundtripJSON() throws JsonProcessingException
	{
		String officeId = "SWT";
		String ratingId = "ARBU.Elev;Stor.Linear.Production";

		RatingSpec spec = buildRatingSpec(officeId, ratingId);

		ObjectMapper om = JsonV1.buildObjectMapper();
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

		String dateMethods = "LINEAR,NEAREST,NEAREST";

		retval = new RatingSpec.Builder().withOfficeId(officeId).withRatingId(ratingId)
				.withTemplateId(templateId).withLocationId(locId).withVersion(version).withSourceAgency(agency)
				.withActive(activeFlag).withAutoUpdate(autoUpdateFlag).withAutoActivate(autoActivateFlag)
				.withAutoMigrateExtension(autoMigrateExtFlag)
				//				.withIndRoundingSpecsString(indRndSpecs)
				.withIndRoundingSpecs(buildIndRoundingSpecs(indRndSpecs))
				.withDepRoundingSpec(depRndSpecs).withDescription(desc)
				.withDateMethods(dateMethods)
				.build();

		return retval;
	}

}