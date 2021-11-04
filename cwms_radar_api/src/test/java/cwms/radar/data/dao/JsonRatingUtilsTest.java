package cwms.radar.data.dao;

import java.io.InputStream;
import java.util.Scanner;

import org.junit.jupiter.api.Test;

import hec.data.RatingException;
import hec.data.cwmsRating.RatingSet;

import static org.junit.jupiter.api.Assertions.*;

class JsonRatingUtilsTest
{

	public String loadResourceAsString(String fileName)
	{
		ClassLoader classLoader = getClass().getClassLoader();
		InputStream stream = classLoader.getResourceAsStream(fileName);
		assertNotNull(stream, "Could not load the resource as stream:" + fileName);
		Scanner scanner = new Scanner(stream);
		String contents = scanner.useDelimiter("\\A").next();
		scanner.close();
		return contents;
	}

	@Test
	void test_xml_to_json_to_rating_set() throws RatingException
	{
		String xmlRating = loadResourceAsString("cwms/radar/data/dao/rating.xml");
		// make sure we got something.
		assertNotNull(xmlRating);

		// make sure we can parse it.
		RatingSet ratingSet = RatingSet.fromXml(xmlRating);
		assertNotNull(ratingSet);

		// turn it into json
		String json = JsonRatingUtils.toJson(ratingSet);
		assertNotNull(json);
		assertFalse(json.isEmpty());

		// turn json into a rating set
		RatingSet ratingSet2 = JsonRatingUtils.fromJson(json);
		assertNotNull(ratingSet2);

		assertEquals(ratingSet.getName(), ratingSet2.getName());

		assertEquals(ratingSet.toXmlString(" "), ratingSet2.toXmlString(" "));
	}

}