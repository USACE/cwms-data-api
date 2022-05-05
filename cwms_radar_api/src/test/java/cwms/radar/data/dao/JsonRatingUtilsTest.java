package cwms.radar.data.dao;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import hec.data.RatingException;
import hec.data.cwmsRating.RatingSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

public class JsonRatingUtilsTest
{

	public static String loadResourceAsString(String fileName) throws IOException
	{
		String retval = null;
		ClassLoader classLoader = JsonRatingUtilsTest.class.getClassLoader();

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

	public static String readFully(InputStream inputStream) throws IOException
	{
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		byte[] buffer = new byte[2048];
		for(int length; (length = inputStream.read(buffer)) != -1; )
		{
			result.write(buffer, 0, length);
		}
		// StandardCharsets.UTF_8.name() > JDK 7
		return result.toString("UTF-8");
	}

	@Test
	void test_xml_to_json_to_rating_set() throws RatingException, IOException
	{
		String[] files = {			"rating.xml.gz" };
		roundTripFilesThruJson(files);
	}

	@Disabled("Very slow")
	@Test
	void test_xml_to_json_to_rating_set_assorted() throws RatingException, IOException
	{
		String[] files = {
				"ARBU.Elev_Stor.Linear.Production.xml.gz",
				"DICK.Stage_Flow.EXSA.PRODUCTION.xml.gz",
				"LENA.Stage_Flow.BASE.PRODUCTION.xml.gz",
				"SMNM_Stage_Flow_Linear_Step.xml",
				"TOMS.Opening-Conduit_Gates_Elev_Flow-Conduit_Gates.Standard.Production.xml.gz",};
		roundTripFilesThruJson(files);
	}

	@Disabled("Very slow")
	@Test
	void test_xml_to_json_to_rating_set_SPK() throws RatingException, IOException
	{
		String[] files = {
				"Black_Butte-Pool_Elev_Area_Standard_Production.xml.gz",
				"Black_Rascal_Div_Stage_Flow_USGS-EXSA_Production.xml",
				"Black_Rascal_Div_Stage_Flow_USGS-EXSA_Production_2018-12-21_0800.xml",
				"Farmington_Dam-Gate_1_Opening-Gate_Elev_Flow_Standard_Production.xml",
				"Pine_Flat_Lake-Pool_Elev_Area_Standard_Production.xml.gz",};
		roundTripFilesThruJson(files);
	}

	@Disabled("Very slow")
	@Test
	void test_xml_to_json_to_rating_set_NWO() throws RatingException, IOException
	{
		String[] files = {
				"BOHA-GateMidLevel_Opening_Elev_Flow_Linear_Step.xml",
				"ECMT_Stage_Stage_Linear_StepCorrections.xml",
				"FTPK-Fort_Peck_Dam-Missouri_Elev-Estimated_Stor_USGS-EXSA_Production.xml.gz",
				"SMNM_Stage_Flow_Linear_Step.xml",
				"YETL_Elev_Stor_Linear_Step.xml.gz"};
		roundTripFilesThruJson(files);
	}

	private void roundTripFilesThruJson(String[] files) throws RatingException, IOException
	{
		Arrays.stream(files).forEach(this::roundtripThruJson);
	}

	private void roundtripThruJson(String filename)
	{
		String xmlRating = null;
		try
		{
			xmlRating = loadResourceAsString("cwms/radar/data/dao/" + filename);
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
		catch(IOException | RatingException e)
		{
			fail("Could not roundtrip file:" + filename, e);
		}
	}

}
