package cwms.cda.data.dao;

import static cwms.cda.data.dao.JsonRatingUtils.buildSourceFromResource;
import static org.junit.jupiter.api.Assertions.*;

import hec.data.RatingException;
import hec.data.cwmsRating.RatingSet;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

import mil.army.usace.hec.cwms.rating.io.xml.RatingXmlFactory;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.xml.transform.Source;
import javax.xml.transform.TransformerException;

public class JsonRatingUtilsTest
{

	public static String loadResourceAsString(String fileName) throws IOException
	{
		String retval = null;
		ClassLoader classLoader = JsonRatingUtilsTest.class.getClassLoader();

		if(fileName != null)
		{
			InputStream stream = classLoader.getResourceAsStream(fileName);
			if(fileName.endsWith(".gz") && stream != null)
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

		return result.toString(StandardCharsets.UTF_8);
	}

	@Test
	void test_xml_to_json_to_rating_set()
	{
		String[] files = {			"rating.xml.gz" };
		roundTripFilesThruJson(files);
	}

	@Tag("slow")  // 6s
	@Test
	void test_xml_to_json_to_rating_set_assorted()
	{
		String[] files = {
				"ARBU.Elev_Stor.Linear.Production.xml.gz",
				"DICK.Stage_Flow.EXSA.PRODUCTION.xml.gz",
				"LENA.Stage_Flow.BASE.PRODUCTION.xml.gz",
				"SMNM_Stage_Flow_Linear_Step.xml",
				"TOMS.Opening-Conduit_Gates_Elev_Flow-Conduit_Gates.Standard.Production.xml.gz",};
		roundTripFilesThruJson(files);
	}

	@Tag("slow")  // 22 sec
	@Test
	void test_xml_to_json_to_rating_set_SPK()
	{
		String[] files = {
				"Black_Butte-Pool_Elev_Area_Standard_Production.xml.gz",
				"Black_Rascal_Div_Stage_Flow_USGS-EXSA_Production.xml",
				"Black_Rascal_Div_Stage_Flow_USGS-EXSA_Production_2018-12-21_0800.xml",
				"Farmington_Dam-Gate_1_Opening-Gate_Elev_Flow_Standard_Production.xml",
				"Pine_Flat_Lake-Pool_Elev_Area_Standard_Production.xml.gz",};
		roundTripFilesThruJson(files);
	}

	@Tag("slow")  //20 sec
	@Test
	void test_xml_to_json_to_rating_set_NWO()
	{
		String[] files = {
				"BOHA-GateMidLevel_Opening_Elev_Flow_Linear_Step.xml",
				"ECMT_Stage_Stage_Linear_StepCorrections.xml",
				"FTPK-Fort_Peck_Dam-Missouri_Elev-Estimated_Stor_USGS-EXSA_Production.xml.gz",
				"SMNM_Stage_Flow_Linear_Step.xml",
				"YETL_Elev_Stor_Linear_Step.xml.gz"};
		roundTripFilesThruJson(files);
	}

	private void roundTripFilesThruJson(String[] files) {
		Arrays.stream(files).forEach(this::roundtripFileThruJson);
	}

	private void roundtripFileThruJson(String filename)
	{
		String xmlRating;
		try
		{
			xmlRating = loadResourceAsString("cwms/cda/data/dao/" + filename);
            roundtripThruJson(xmlRating);
        }
		catch(IOException | RatingException e)
		{
			fail("Could not roundtrip file:" + filename, e);
		}
	}

    private static void roundtripThruJson(String xmlRating) throws RatingException {
        // make sure we got something.
        assertNotNull(xmlRating);

        // make sure we can parse it.
        RatingSet ratingSet = RatingXmlFactory.ratingSet(xmlRating);
        assertNotNull(ratingSet);

        // turn it into json
        String json = JsonRatingUtils.toJson(ratingSet);
        assertNotNull(json);
        assertFalse(json.isEmpty());

        // turn json into a rating set
        RatingSet ratingSet2 = JsonRatingUtils.fromJson(json);
        assertNotNull(ratingSet2);

        assertEquals(ratingSet.getName(), ratingSet2.getName());

        assertEquals(RatingXmlFactory.toXml(ratingSet, " "),
                RatingXmlFactory.toXml(ratingSet2," "));
    }

    @Test
    void test_just_farm()
    {
        String file = "Farmington_Dam-Gate_1_Opening-Gate_Elev_Flow_Standard_Production.xml";
        roundtripFileThruJson(file);
    }

    @Test
    void test_just_ind() throws IOException, RatingException {
        String xmlRating = loadResourceAsString("cwms/cda/api/spk/ratings_ind.xml");
        roundtripThruJson(xmlRating);
    }

    @Test
    void test_json_to_xml_ind() throws IOException, TransformerException {
        String json = loadResourceAsString("cwms/cda/api/spk/ratings_ind.json");
        String asXml = JsonRatingUtils.jsonToXml(json);
        assertTrue(asXml.contains("other-ind position=\"2\""));

    }

    @Test
    void test_move_value() throws IOException, TransformerException {
        String xml = loadResourceAsString("cwms/cda/api/spk/pre_move_value.xml");
        assertTrue(xml.contains("<other-ind position=\"2\">"));

        Source xslt = buildSourceFromResource("move_value.xsl");
        String xformed = JsonRatingUtils.applyTransform(xml, xslt);

        assertTrue(xformed.contains("other-ind position=\"2\""));
    }

}
