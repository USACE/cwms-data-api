package cwms.cda.data.dao;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import cwms.cda.data.dto.rating.document.HeightShiftsElement;
import cwms.cda.data.dto.rating.document.IndParameterSpecElement;
import cwms.cda.data.dto.rating.document.PointElement;
import cwms.cda.data.dto.rating.document.PositionalValueElement;
import cwms.cda.data.dto.rating.document.RatingPointsElement;
import cwms.cda.data.dto.rating.document.RatingSpecElement;
import cwms.cda.data.dto.rating.document.RatingTemplateElement;
import cwms.cda.data.dto.rating.document.RatingsDocument;
import cwms.cda.data.dto.rating.document.SimpleRatingElement;
import cwms.cda.data.dto.rating.document.SourceRatingElement;
import cwms.cda.data.dto.rating.document.TransitionalRatingElement;
import cwms.cda.data.dto.rating.document.UsgsStreamRatingElement;
import cwms.cda.data.dto.rating.document.VirtualRatingElement;
import hec.data.RatingException;
import hec.data.cwmsRating.RatingSet;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

import mil.army.usace.hec.cwms.rating.io.xml.RatingXmlFactory;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
    void test_json_to_xml_ind() throws IOException {
        String json = loadResourceAsString("cwms/cda/api/spk/ratings_ind.json");
        String asXml = JsonRatingUtils.jsonToXml(json);
        assertTrue(asXml.contains("other-ind position=\"2\""));
        // attributes, not child elements
        assertTrue(asXml.contains("<rating-template office-id=\"SPK\">"));
        assertTrue(asXml.contains("<ind-rounding-spec position=\"1\">2222233332</ind-rounding-spec>"));
        assertTrue(asXml.contains("<offset estimate=\"true\">"));
        assertFalse(asXml.contains("element-value"));
        assertFalse(asXml.contains("noNamespaceSchemaLocation"));
    }

    @Test
    void test_read_json_dto() throws IOException {
        String json = loadResourceAsString("cwms/cda/api/spk/ratings_ind.json");
        RatingsDocument ratings = JsonRatingUtils.readJson(json);
        assertDtoMatchesRatingsInd(ratings);
    }

    @Test
    void test_read_xml_dto() throws IOException {
        String xml = loadResourceAsString("cwms/cda/api/spk/ratings_ind.xml");
        RatingsDocument ratings = JsonRatingUtils.readXml(xml);
        assertDtoMatchesRatingsInd(ratings);
    }

    private static void assertDtoMatchesRatingsInd(RatingsDocument ratings) {
        assertEquals(1, ratings.getRatingTemplates().size());
        assertEquals("SPK", ratings.getRatingTemplates().get(0).getOfficeId());
        assertEquals(3, ratings.getRatingTemplates().get(0).getIndParameterSpecs()
                .getIndParameterSpecs().size());

        RatingSpecElement spec = ratings.getRatingSpecs().get(0);
        assertEquals("Barren", spec.getLocationId());
        assertEquals("", spec.getSourceAgency());
        PositionalValueElement rounding = spec.getIndRoundingSpecs().getIndRoundingSpecs().get(1);
        assertEquals("2", rounding.getPosition());
        assertEquals("2222233332", rounding.getValue());

        SimpleRatingElement simple = ratings.getSimpleRatings().get(0);
        assertEquals("SPK", simple.getOfficeId());
        assertEquals("ft", simple.getVerticalDatumInfo().getUnit());
        assertEquals("NGVD-29", simple.getVerticalDatumInfo().getNativeDatum());
        assertEquals(2, simple.getVerticalDatumInfo().getOffsets().size());
        assertEquals("true", simple.getVerticalDatumInfo().getOffsets().get(1).getEstimate());
        assertEquals("-0.4403", simple.getVerticalDatumInfo().getOffsets().get(1).getValue());

        RatingPointsElement points = simple.getRatingPoints().get(0);
        assertEquals("2", points.getOtherInds().get(1).getPosition());
        assertEquals("0.0", points.getOtherInds().get(1).getValue());
        assertEquals("510.0", points.getPoints().get(0).getInd());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "cwms/cda/api/spk/ratings_ind.xml",
        "cwms/cda/data/dao/rating.xml.gz",
        "cwms/cda/data/dao/ECMT_Stage_Stage_Linear_StepCorrections.xml",
        "cwms/cda/data/dao/Black_Rascal_Div_Stage_Flow_USGS-EXSA_Production.xml",
        "cwms/cda/data/dao/BEAV.Stage_Flow.BASE.PRODUCTION.xml",
        "cwms/cda/data/dao/Farmington_Dam-Gate_1_Opening-Gate_Elev_Flow_Standard_Production.xml",
        "cwms/cda/api/STJ-St_Joseph-Missouri_Stage_Flow_USGS-BASE_Production_2.xml",
    })
    void test_json_matches_legacy_format(String resource) throws IOException {
        String xml = loadResourceAsString(resource);

        String legacyJson = new ObjectMapper().writeValueAsString(new XmlMapper().readTree(xml))
                .replace("\"\":", "\"element-value\":");
        JsonNode expected = new ObjectMapper().readTree(legacyJson);

        JsonNode actual = new ObjectMapper().readTree(JsonRatingUtils.xmlToJson(xml));

        assertEquals(expected, actual);
    }

    @Test
    void test_single_element_lists_unwrapped_in_json() throws IOException {
        String xml = loadResourceAsString("cwms/cda/data/dao/ECMT_Stage_Stage_Linear_StepCorrections.xml");
        String json = JsonRatingUtils.xmlToJson(xml);
        JsonNode node = new ObjectMapper().readTree(json);

        // only one of each in the document so they are objects, not arrays
        assertTrue(node.get("rating-template").isObject());
        assertTrue(node.get("simple-rating").isObject());
        assertTrue(node.get("rating-spec").get("ind-rounding-specs").get("ind-rounding-spec").isObject());
        assertTrue(node.get("simple-rating").get("rating-points").get("point").isArray());

        // and they can be read back either as an object or a single element array
        RatingsDocument ratings = JsonRatingUtils.readJson(json);
        assertEquals(1, ratings.getSimpleRatings().size());
        assertEquals(1, ratings.getRatingSpecs().get(0).getIndRoundingSpecs().getIndRoundingSpecs().size());
    }

    @Test
    void test_empty_element_is_empty_string_in_json() throws IOException {
        String xml = "<ratings>"
                + "<usgs-stream-rating office-id=\"LRL\">"
                + "<rating-spec-id>A.Stage;Flow.Logarithmic.USGS-NWIS</rating-spec-id>"
                + "<units-id>ft;cfs</units-id>"
                + "<effective-date>2013-10-01T04:00:00Z</effective-date>"
                + "<active>true</active>"
                + "<height-offsets/>"
                + "<rating-points/>"
                + "</usgs-stream-rating>"
                + "</ratings>";

        String json = JsonRatingUtils.xmlToJson(xml);
        JsonNode usgs = new ObjectMapper().readTree(json).get("usgs-stream-rating");
        assertTrue(usgs.get("rating-points").isTextual());
        assertEquals("", usgs.get("rating-points").asText());
        assertTrue(usgs.get("height-offsets").isTextual());
        assertEquals("", usgs.get("height-offsets").asText());

        RatingsDocument ratings = JsonRatingUtils.readJson(json);
        assertEquals(1, ratings.getUsgsStreamRatings().get(0).getRatingPoints().size());
        assertNotNull(ratings.getUsgsStreamRatings().get(0).getHeightOffsets());

        RatingsDocument reread = JsonRatingUtils.readXml(JsonRatingUtils.writeXml(ratings));
        assertEquals(1, reread.getUsgsStreamRatings().get(0).getRatingPoints().size());
        assertNotNull(reread.getUsgsStreamRatings().get(0).getHeightOffsets());
    }

    @Test
    void test_interleaved_rating_types_are_accumulated() throws IOException {
        String xml = "<ratings>"
                + "<simple-rating office-id=\"SWT\"><rating-spec-id>A.Stage;Flow.Linear.Production</rating-spec-id>"
                + "<effective-date>2020-01-01T00:00:00Z</effective-date><active>true</active>"
                + "<formula>i1 * 2</formula></simple-rating>"
                + "<virtual-rating office-id=\"SWT\"><rating-spec-id>A.Stage;Flow.Virtual.Production</rating-spec-id>"
                + "<effective-date>2021-01-01T00:00:00Z</effective-date><active>true</active>"
                + "<connections>R2I1=R1D</connections><source-ratings>"
                + "<source-rating position=\"1\"><rating-spec-id>A.Stage;Stage.Linear.Production {ft;ft}</rating-spec-id></source-rating>"
                + "<source-rating position=\"2\"><rating-expression>I1 * 2 {ft;cfs}</rating-expression></source-rating>"
                + "</source-ratings></virtual-rating>"
                + "<simple-rating office-id=\"SWT\"><rating-spec-id>A.Stage;Flow.Linear.Production</rating-spec-id>"
                + "<effective-date>2022-01-01T00:00:00Z</effective-date><active>true</active>"
                + "<formula>i1 * 3</formula></simple-rating>"
                + "</ratings>";

        RatingsDocument ratings = JsonRatingUtils.readXml(xml);
        assertEquals(2, ratings.getSimpleRatings().size());
        assertEquals("i1 * 3", ratings.getSimpleRatings().get(1).getFormula());

        VirtualRatingElement virtual = ratings.getVirtualRatings().get(0);
        assertEquals("R2I1=R1D", virtual.getConnections());
        assertEquals(2, virtual.getSourceRatings().getSourceRatings().size());
        assertEquals("I1 * 2 {ft;cfs}", virtual.getSourceRatings().getSourceRatings().get(1).getRatingExpression());
    }

    @Test
    void test_transitional_rating_round_trip() throws IOException {
        String xml = "<ratings>"
                + "<transitional-rating office-id=\"SWT\">"
                + "<rating-spec-id>A.Stage;Flow.Transitional.Production</rating-spec-id>"
                + "<units-id>ft;cfs</units-id>"
                + "<effective-date>1900-01-01T00:00:00-06:00</effective-date>"
                + "<transition-start-date/>"
                + "<active>true</active>"
                + "<select><case position=\"1\"><when>I1 GT 25</when><then>R1</then></case><default>R2</default></select>"
                + "<source-ratings>"
                + "<rating-spec-id position=\"1\">A.Stage;Flow.Linear.Dummy</rating-spec-id>"
                + "<rating-spec-id position=\"2\">A.Stage;Flow.Linear.Production</rating-spec-id>"
                + "</source-ratings>"
                + "</transitional-rating>"
                + "</ratings>";

        RatingsDocument ratings = JsonRatingUtils.readXml(xml);
        TransitionalRatingElement transitional = ratings.getTransitionalRatings().get(0);
        assertEquals("", transitional.getTransitionStartDate());
        assertEquals("I1 GT 25", transitional.getSelect().getCases().get(0).getWhen());
        assertEquals("R2", transitional.getSelect().getDefault());
        assertEquals("2", transitional.getSourceRatings().getRatingSpecIds().get(1).getPosition());
        assertEquals("A.Stage;Flow.Linear.Production", transitional.getSourceRatings().getRatingSpecIds().get(1).getValue());

        // xml -> dto -> json -> dto -> xml is stable
        String dbXml = JsonRatingUtils.writeXml(ratings);
        String viaJson = JsonRatingUtils.jsonToXml(JsonRatingUtils.writeJson(ratings));
        assertEquals(dbXml, viaJson);
    }

    @Test
    void test_schema_location_kept_in_json_dropped_in_xml() throws IOException, RatingException {
        String xml = loadResourceAsString("cwms/cda/data/dao/ECMT_Stage_Stage_Linear_StepCorrections.xml");
        RatingsDocument ratings = JsonRatingUtils.readXml(xml);
        assertEquals("http://www.hec.usace.army.mil/xmlSchema/cwms/Ratings.xsd",
                ratings.getNoNamespaceSchemaLocation());
        assertTrue(JsonRatingUtils.writeJson(ratings).contains("noNamespaceSchemaLocation"));

        String dbXml = JsonRatingUtils.writeXml(ratings);
        assertFalse(dbXml.contains("noNamespaceSchemaLocation"));
        assertNotNull(JsonRatingUtils.toRatingSet(ratings));
    }

    @Test
    void test_remove_templates() throws IOException {
        String xml = loadResourceAsString("cwms/cda/api/spk/ratings_ind.xml");
        RatingsDocument ratings = JsonRatingUtils.readXml(xml);
        RatingsDocument noTemplates = new RatingsDocument.Builder(ratings).withRatingTemplates(null).build();

        String dbXml = JsonRatingUtils.writeXml(noTemplates);
        assertFalse(dbXml.contains("rating-template"));
        assertTrue(dbXml.contains("<rating-spec office-id=\"SPK\">"));
        assertTrue(dbXml.contains("<simple-rating office-id=\"SPK\">"));
    }

    @Test
    void test_unknown_field_rejected() {
        String json = "{\"simple-rating\": {\"office-id\": \"SWT\", \"not-a-field\": \"x\"}}";
        assertThrows(IOException.class, () -> JsonRatingUtils.readJson(json));
    }

    @Test
    void test_read_retrieved_ratings_json() throws IOException, RatingException {
        // json as returned by GET /ratings
        String json = loadResourceAsString("cwms/cda/data/dto/rating/rating_post.json");
        RatingsDocument ratings = JsonRatingUtils.readJson(json);

        RatingTemplateElement template = ratings.getRatingTemplates().get(0);
        assertEquals("SPK", template.getOfficeId());
        assertEquals("Stage;Flow", template.getParametersId());
        assertEquals("EXSA", template.getVersion());
        assertEquals("Flow", template.getDepParameter());
        assertEquals("", template.getDescription());
        IndParameterSpecElement indSpec = template.getIndParameterSpecs().getIndParameterSpecs().get(0);
        assertEquals("1", indSpec.getPosition());
        assertEquals("Stage", indSpec.getParameter());
        assertEquals("LINEAR", indSpec.getInRangeMethod());
        assertEquals("NEAREST", indSpec.getOutRangeLowMethod());
        assertEquals("NEAREST", indSpec.getOutRangeHighMethod());

        RatingSpecElement spec = ratings.getRatingSpecs().get(0);
        assertEquals("SPK", spec.getOfficeId());
        assertEquals("FSMI.Stage;Flow.EXSA.PRODUCTION", spec.getRatingSpecId());
        assertEquals("Stage;Flow.EXSA", spec.getTemplateId());
        assertEquals("FSMI", spec.getLocationId());
        assertEquals("PRODUCTION", spec.getVersion());
        assertEquals("", spec.getSourceAgency());
        assertEquals("PREVIOUS", spec.getInRangeMethod());
        assertEquals("NEAREST", spec.getOutRangeLowMethod());
        assertEquals("PREVIOUS", spec.getOutRangeHighMethod());
        assertEquals("true", spec.getActive());
        assertEquals("false", spec.getAutoUpdate());
        PositionalValueElement indRounding = spec.getIndRoundingSpecs().getIndRoundingSpecs().get(0);
        assertEquals("1", indRounding.getPosition());
        assertEquals("2223456784", indRounding.getValue());
        assertEquals("2223456784", spec.getDepRoundingSpec());

        SimpleRatingElement simple = ratings.getSimpleRatings().get(0);
        assertEquals("SPK", simple.getOfficeId());
        assertEquals("FSMI.Stage;Flow.EXSA.PRODUCTION", simple.getRatingSpecId());
        assertEquals("ft;cfs", simple.getUnitsId());
        assertEquals("2007-07-11T20:38:00Z", simple.getEffectiveDate());
        assertEquals("true", simple.getActive());
        assertEquals(1, simple.getRatingPoints().size());
        assertNull(simple.getRatingPoints().get(0).getOtherInds());
        List<PointElement> points = simple.getRatingPoints().get(0).getPoints();
        assertEquals(12, points.size());
        assertEquals("17", points.get(0).getInd());
        assertEquals("114000", points.get(0).getDep());
        assertEquals("18.75", points.get(1).getInd());
        assertEquals("520000", points.get(11).getDep());

        String dbXml = JsonRatingUtils.writeXml(ratings);
        assertTrue(dbXml.contains("<rating-template office-id=\"SPK\">"));
        assertTrue(dbXml.contains("<rating-spec office-id=\"SPK\">"));
        assertTrue(dbXml.contains("<simple-rating office-id=\"SPK\">"));
        assertTrue(dbXml.contains("<ind-rounding-spec position=\"1\">2223456784</ind-rounding-spec>"));
        assertFalse(dbXml.contains("query-info"));

        RatingSet ratingSet = JsonRatingUtils.toRatingSet(ratings);
        assertEquals("FSMI.Stage;Flow.EXSA.PRODUCTION", ratingSet.getRatingSpec().getRatingSpecId());
    }

    @Test
    void test_read_retrieved_ratings_json_multiple_independent() throws IOException {
        String json = loadRetrieved("multiple_independent.json");

        SimpleRatingElement simple = JsonRatingUtils.readJson(json).getSimpleRatings().get(0);
        assertEquals("ft,ft;cfs", simple.getUnitsId());
        assertEquals(2, simple.getRatingPoints().size());
        RatingPointsElement second = simple.getRatingPoints().get(1);
        assertEquals("1", second.getOtherInds().get(0).getPosition());
        assertEquals("2", second.getOtherInds().get(0).getValue());
        assertEquals(2, second.getPoints().size());
        assertEquals("101", second.getPoints().get(1).getInd());
        assertEquals("40", second.getPoints().get(1).getDep());
    }

    @Test
    void test_read_retrieved_ratings_json_rejects_bad_rows() {
        String json = "{\"ratings\": {\"ratings\": [{\"simple-rating\": {"
                + "\"name\": \"A.Stage;Flow.Linear.Production\", \"ind-parameters\": [\"Stage (ft)\"],"
                + "\"dep-parameter\": \"Flow (cfs)\", \"values\": [[1, 2, 3]]}}]}}";
        assertThrows(IOException.class, () -> JsonRatingUtils.readJson(json));
    }

    @Test
    void test_read_retrieved_ratings_json_rejects_lossy_multiple_independent() {
        // Ratings_v1_to_RADAR_json.xsl drops the other-ind values, leaving [ind, dep] rows
        String json = "{\"ratings\": {\"ratings\": [{\"simple-rating\": {"
                + "\"name\": \"A.Opening,Elev;Flow.Standard.Production\","
                + "\"ind-parameters\": [\"Opening (ft)\", \"Elev (ft)\"], \"dep-parameter\": \"Flow (cfs)\","
                + "\"values\": [[100, 10], [101, 20]]}}]}}";
        IOException ex = assertThrows(IOException.class, () -> JsonRatingUtils.readJson(json));
        assertTrue(ex.getMessage().contains("GET /ratings/{rating-id}"));
    }

    @Test
    void test_read_retrieved_ratings_json_rejects_unknown_rating_type() {
        String json = "{\"ratings\": {\"ratings\": [{\"not-a-rating\": "
                + "{\"name\": \"A.Stage;Flow.Linear.Production\"}}]}}";
        assertThrows(IOException.class, () -> JsonRatingUtils.readJson(json));
    }

    @Test
    void test_read_retrieved_ratings_json_rejects_error() {
        String json = "{\"ratings\":{\"error\":\"Query exceeded maximum time of +00 00:00:30\"}}";
        IOException ex = assertThrows(IOException.class, () -> JsonRatingUtils.readJson(json));
        assertTrue(ex.getMessage().contains("Query exceeded maximum time"));
    }

    @Test
    void test_read_retrieved_simple_rating_formula_notes_and_datum() throws IOException {
        String json = loadRetrieved("simple_formula_notes_datum.json");

        List<SimpleRatingElement> ratings = JsonRatingUtils.readJson(json).getSimpleRatings();
        SimpleRatingElement table = ratings.get(0);
        assertEquals("ft;ac-ft", table.getUnitsId());
        List<PointElement> points = table.getRatingPoints().get(0).getPoints();
        assertNull(points.get(0).getNote());
        assertEquals("510", points.get(1).getInd());
        assertEquals("spillway", points.get(1).getNote());

        SimpleRatingElement formula = ratings.get(1);
        assertEquals("i1 * 2", formula.getFormula());
        assertNull(formula.getRatingPoints());
    }

    @Test
    void test_read_retrieved_usgs_stream_rating() throws IOException, RatingException {
        String json = loadResourceAsString("cwms/cda/data/dto/rating/rating_post_usgs.json");
        RatingsDocument ratings = JsonRatingUtils.readJson(json);

        assertEquals("LOGARITHMIC", ratings.getRatingTemplates().get(0).getIndParameterSpecs()
                .getIndParameterSpecs().get(0).getInRangeMethod());
        assertEquals("USGS", ratings.getRatingSpecs().get(0).getSourceAgency());

        UsgsStreamRatingElement usgs = ratings.getUsgsStreamRatings().get(0);
        assertEquals("SPK", usgs.getOfficeId());
        assertEquals("USGSTEST.Stage;Flow.USGS-EXSA.USGS-NWIS", usgs.getRatingSpecId());
        assertEquals("ft;cfs", usgs.getUnitsId());
        assertEquals("2020-01-01T00:00:00Z", usgs.getEffectiveDate());
        assertEquals("true", usgs.getActive());

        HeightShiftsElement shift = usgs.getHeightShifts().get(0);
        assertEquals("2020-06-01T00:00:00Z", shift.getEffectiveDate());
        assertEquals(2, shift.getPoints().size());
        assertEquals("5", shift.getPoints().get(1).getInd());
        assertEquals("0.2", shift.getPoints().get(1).getDep());
        assertEquals("MANUAL", shift.getPoints().get(1).getNote());

        assertEquals("0.5", usgs.getHeightOffsets().getPoints().get(0).getDep());

        List<PointElement> points = usgs.getRatingPoints().get(0).getPoints();
        assertEquals(4, points.size());
        assertEquals("0.6", points.get(0).getInd());
        assertEquals("300", points.get(3).getDep());
        assertEquals("BASE", points.get(3).getNote());
        assertNull(ratings.getSimpleRatings());

        String dbXml = JsonRatingUtils.writeXml(ratings);
        assertTrue(dbXml.contains("<usgs-stream-rating office-id=\"SPK\">"));
        assertTrue(dbXml.contains("<height-shifts>"));
        assertTrue(dbXml.contains("<height-offsets>"));

        RatingSet ratingSet = JsonRatingUtils.toRatingSet(ratings);
        assertEquals("USGSTEST.Stage;Flow.USGS-EXSA.USGS-NWIS", ratingSet.getRatingSpec().getRatingSpecId());
    }

    @Test
    void test_read_retrieved_usgs_stream_rating_without_points() throws IOException {
        // A rating holding only shifts for an existing base curve
        String json = loadRetrieved("usgs_without_points.json");

        RatingsDocument ratings = JsonRatingUtils.readJson(json);
        UsgsStreamRatingElement usgs = ratings.getUsgsStreamRatings().get(0);
        assertNull(usgs.getHeightOffsets().getPoints());
        assertNull(usgs.getRatingPoints().get(0).getPoints());

        String dbXml = JsonRatingUtils.writeXml(ratings);
        assertTrue(dbXml.contains("<height-offsets/>"));
        assertTrue(dbXml.contains("<rating-points/>"));
    }

    @Test
    void test_read_retrieved_virtual_rating() throws IOException {
        String json = loadRetrieved("virtual.json");

        RatingsDocument ratings = JsonRatingUtils.readJson(json);
        VirtualRatingElement virtual = ratings.getVirtualRatings().get(0);
        assertEquals("SWT", virtual.getOfficeId());
        assertEquals("A.Elev;Flow.Virtual.Production", virtual.getRatingSpecId());
        assertEquals("R2I1=R1D,R3I1=R2D", virtual.getConnections());

        List<SourceRatingElement> sources = virtual.getSourceRatings().getSourceRatings();
        assertEquals(3, sources.size());
        assertEquals("1", sources.get(0).getPosition());
        assertEquals("A.Elev;Stage.Linear.Production {ft;ft}", sources.get(0).getRatingSpecId());
        assertNull(sources.get(0).getRatingExpression());
        assertEquals("2", sources.get(1).getPosition());
        assertEquals("I1 * 2.5 {ft;cfs}", sources.get(1).getRatingExpression());
        assertNull(sources.get(1).getRatingSpecId());
        assertEquals("A.Flow;Flow.Linear.Production {cfs;cfs}", sources.get(2).getRatingSpecId());

        String dbXml = JsonRatingUtils.writeXml(ratings);
        assertTrue(dbXml.contains("<source-rating position=\"2\">"));
        assertTrue(dbXml.contains("<rating-expression>I1 * 2.5 {ft;cfs}</rating-expression>"));
    }

    @Test
    void test_read_retrieved_transitional_rating() throws IOException {
        String json = loadRetrieved("transitional.json");

        RatingsDocument ratings = JsonRatingUtils.readJson(json);
        TransitionalRatingElement transitional = ratings.getTransitionalRatings().get(0);
        assertEquals("SWT", transitional.getOfficeId());
        assertEquals("ft;cfs", transitional.getUnitsId());
        assertEquals("1900-01-01T06:00:00Z", transitional.getEffectiveDate());
        assertEquals(2, transitional.getSelect().getCases().size());
        assertEquals("2", transitional.getSelect().getCases().get(1).getPosition());
        assertEquals("I1 GT 10", transitional.getSelect().getCases().get(1).getWhen());
        assertEquals("R2", transitional.getSelect().getCases().get(1).getThen());
        assertEquals("R2", transitional.getSelect().getDefault());
        List<PositionalValueElement> sources = transitional.getSourceRatings().getRatingSpecIds();
        assertEquals("1", sources.get(0).getPosition());
        assertEquals("A.Stage;Flow.Linear.Dummy", sources.get(0).getValue());
        assertEquals("2", sources.get(1).getPosition());
        assertEquals("A.Stage;Flow.Linear.Production", sources.get(1).getValue());

        String dbXml = JsonRatingUtils.writeXml(ratings);
        assertTrue(dbXml.contains("<rating-spec-id position=\"2\">A.Stage;Flow.Linear.Production</rating-spec-id>"));
    }

    @Test
    void test_read_retrieved_mixed_rating_types() throws IOException {
        String json = loadRetrieved("mixed_types.json");

        RatingsDocument ratings = JsonRatingUtils.readJson(json);
        assertEquals(1, ratings.getSimpleRatings().size());
        assertEquals(1, ratings.getTransitionalRatings().size());
        assertNull(ratings.getTransitionalRatings().get(0).getSelect().getCases());
    }

    // json in the format returned by GET /ratings
    private static String loadRetrieved(String fileName) throws IOException {
        return loadResourceAsString("cwms/cda/data/dto/rating/retrieved/" + fileName);
    }

}
