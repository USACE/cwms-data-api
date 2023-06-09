package cwms.cda.data.dto;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;

import cwms.cda.data.dao.JsonRatingUtilsTest;
import cwms.cda.data.dto.rating.RatingTemplate;
import cwms.cda.formatters.json.JsonV1;
import hec.data.RatingException;
import hec.data.cwmsRating.RatingSet;
import hec.data.rating.IRatingTemplate;
import java.io.IOException;
import mil.army.usace.hec.cwms.rating.io.xml.RatingXmlFactory;
import org.junit.jupiter.api.Test;


class RatingTemplateTest
{

	@Test
	void test() throws IOException, RatingException
	{
		String filename = "ARBU.Elev_Stor.Linear.Production.xml.gz";

		String xmlRating = JsonRatingUtilsTest.loadResourceAsString("cwms/cda/data/dao/" + filename);
		// make sure we got something.
		assertNotNull(xmlRating);

		// make sure we can parse it.
		RatingSet ratingSet = RatingXmlFactory.ratingSet(xmlRating);
		assertNotNull(ratingSet);

		IRatingTemplate ratingTemplate = ratingSet.getRatingTemplate();

		RatingTemplate.Builder builder = new RatingTemplate.Builder();
		RatingTemplate rt = builder.fromTemplate(ratingTemplate).build();

		ObjectMapper objectMapper = JsonV1.buildObjectMapper();
		String body = objectMapper.writeValueAsString(rt);
		assertNotNull(body);

		RatingTemplate template2 = objectMapper.readValue(body, RatingTemplate.class);
		assertNotNull(template2);

	}
}