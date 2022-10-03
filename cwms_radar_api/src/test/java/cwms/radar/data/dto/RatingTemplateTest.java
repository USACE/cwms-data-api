package cwms.radar.data.dto;

import hec.data.cwmsRating.io.RatingXmlCompatUtil;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.radar.data.dao.JsonRatingUtilsTest;
import cwms.radar.data.dto.rating.RatingTemplate;
import cwms.radar.formatters.json.JsonV1;
import org.junit.jupiter.api.Test;

import hec.data.RatingException;
import hec.data.cwmsRating.RatingSet;
import hec.data.rating.IRatingTemplate;

import static org.junit.jupiter.api.Assertions.assertNotNull;


class RatingTemplateTest
{

	@Test
	void test() throws IOException, RatingException
	{
		String filename = "ARBU.Elev_Stor.Linear.Production.xml.gz";

		String xmlRating = JsonRatingUtilsTest.loadResourceAsString("cwms/radar/data/dao/" + filename);
		// make sure we got something.
		assertNotNull(xmlRating);

		// make sure we can parse it.
		RatingSet ratingSet = RatingXmlCompatUtil.getInstance().createRatingSet(xmlRating);
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