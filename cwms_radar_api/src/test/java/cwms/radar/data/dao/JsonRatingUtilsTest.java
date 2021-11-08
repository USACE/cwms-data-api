package cwms.radar.data.dao;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.Test;

import hec.data.RatingException;
import hec.data.cwmsRating.RatingSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class JsonRatingUtilsTest
{

	public String loadResourceAsString(String fileName) throws IOException
	{
		String retval = null;
		ClassLoader classLoader = getClass().getClassLoader();

		if(fileName != null)
		{
			InputStream stream = classLoader.getResourceAsStream(fileName);
			if(fileName.endsWith(".gz"))
			{
				stream = new GZIPInputStream(stream);
			}
			assertNotNull(stream, "Could not load the resource as stream:" + fileName);
			retval = readFully(stream);
		}
		return retval;
	}

	private String readFully(InputStream inputStream) throws IOException
	{
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		byte[] buffer = new byte[2048];
		for (int length; (length = inputStream.read(buffer)) != -1; ) {
			result.write(buffer, 0, length);
		}
		// StandardCharsets.UTF_8.name() > JDK 7
		return result.toString("UTF-8");
	}

	@Test
	void test_xml_to_json_to_rating_set() throws RatingException, IOException
	{

		String[] files = {
				"rating.xml.gz",
				"ARBU.Elev_Stor.Linear.Production.xml.gz",
				"DICK.Stage_Flow.EXSA.PRODUCTION.xml.gz",
				"LENA.Stage_Flow.BASE.PRODUCTION.xml.gz",
				"TOMS.Opening-Conduit_Gates_Elev_Flow-Conduit_Gates.Standard.Production.xml.gz"
		};
		for(String filename : files)
		{
			roundtripThruJson(filename);
		}
	}

	private void roundtripThruJson(String filename) throws RatingException, IOException
	{
		String xmlRating = loadResourceAsString("cwms/radar/data/dao/" + filename);
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