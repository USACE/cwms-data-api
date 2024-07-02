package cwms.cda.formatters;


import java.util.Arrays;
import java.util.Map;

import cwms.cda.data.dto.Blob;
import cwms.cda.data.dto.Blobs;
import cwms.cda.data.dto.Clob;
import cwms.cda.data.dto.Clobs;
import cwms.cda.data.dto.County;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.Office;
import cwms.cda.data.dto.State;
import cwms.cda.data.dto.basinconnectivity.Basin;
import cwms.cda.formatters.annotations.FormattableWith;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.*;

class FormatsTest
{

	@Test
	void testParseHeaderAndQueryParmJSON(){
		ContentType contentType =Formats.parseHeaderAndQueryParm("application/json", null);

		assertNotNull(contentType);
		assertEquals("application/json", contentType.getType());
		Map<String, String> parameters = contentType.getParameters();
		assertTrue(parameters == null || parameters.isEmpty());

	}

	@Test
	void testParseHeaderAndQueryParmJSONv2()
	{
		ContentType contentType = Formats.parseHeaderAndQueryParm("application/json;version=2", null);

		assertNotNull(contentType);
		assertEquals("application/json", contentType.getType());
		Map<String, String> parameters = contentType.getParameters();
		assertNotNull(parameters);
		assertFalse(parameters.isEmpty());
		assertTrue(parameters.containsKey("version"));
		assertEquals("2", parameters.get("version"));
	}

	@Test
	void testParseNullNull()
	{
		assertThrows(FormattingException.class, () -> Formats.parseHeaderAndQueryParm(null, null));
	}

	@Test
	void testParseEmptyHeader(){

		ContentType contentType =Formats.parseHeaderAndQueryParm("", "json");

		assertNotNull(contentType);
		assertEquals("application/json", contentType.getType());
		Map<String, String> parameters = contentType.getParameters();
		assertTrue(parameters == null || parameters.isEmpty());
	}

	@Test
	void testParseNullHeader(){

		ContentType contentType =Formats.parseHeaderAndQueryParm(null, "json");

		assertNotNull(contentType);
		assertEquals("application/json", contentType.getType());
		Map<String, String> parameters = contentType.getParameters();
		assertTrue(parameters == null || parameters.isEmpty());



	}


	@Test
	void testParseHeaderAndQueryParmXML(){
		assertThrows(FormattingException.class, () -> {
			Formats.parseHeaderAndQueryParm(null, null);
		});

		ContentType contentType =Formats.parseHeaderAndQueryParm("application/xml", null);

		assertNotNull(contentType);
		assertEquals("application/xml", contentType.getType());
		Map<String, String> parameters = contentType.getParameters();
		assertTrue(parameters == null || parameters.isEmpty());


		contentType =Formats.parseHeaderAndQueryParm("application/xml;version=2", null);

		assertNotNull(contentType);
		assertEquals("application/xml", contentType.getType());
		parameters = contentType.getParameters();
		assertNotNull(parameters);
		assertFalse(parameters.isEmpty());
		assertTrue(parameters.containsKey("version"));
		assertEquals("2", parameters.get("version"));

	}

	@Test
	void testParseBoth(){
		assertThrows(FormattingException.class, () -> {
			Formats.parseHeaderAndQueryParm("application/json", "json");
		});


	}

	@Test
	void testParseBothv2(){
		assertThrows(FormattingException.class, () -> {
			Formats.parseHeaderAndQueryParm("application/json;version=2", "json");
		});



	}

	@Test
	void testParseHeader(){
		ContentType contentType;

		contentType = Formats.parseHeader("application/json");
		assertNotNull(contentType);
		assertEquals("application/json", contentType.getType());

		contentType = Formats.parseHeader("application/json;version=2");
		assertNotNull(contentType);
		assertEquals("application/json", contentType.getType());

		assertThrows(FormattingException.class, () -> Formats.parseHeader(null));

		assertThrows(FormattingException.class, () -> Formats.parseHeader(""));

	}

	@EnumSource(ParseQueryParamTest.class)
	@ParameterizedTest
	void testParseQueryParam(ParseQueryParamTest test){
		ContentType contentType = Formats.parseQueryParam(test._contentType, test._class);
		assertEquals(test._expectedType, contentType);
	}

	@Test
	void testParseHeaderAndQueryParmJSONv2WithCharset() {
		ContentType contentType = Formats.parseHeaderAndQueryParm("application/json;version=2; charset=utf-8", null);

		assertNotNull(contentType);
		assertEquals("application/json", contentType.getType());
	}

	@Test
	void testParseHeaderJSONv2WithCharset() {
		ContentType contentType = Formats.parseHeader("application/json;version=2; charset=utf-8");

		assertNotNull(contentType);
		assertEquals("application/json", contentType.getType());
	}

	@Test
	void testParseHeaderFromFirefox()
	{
		//The following header comes from firefox
		ContentType contentType = Formats.parseHeader("text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8");
		assertNotNull(contentType);
		assertEquals(Formats.XML, contentType.toString());
	}

	@EnumSource(ParseHeaderClassAliasTest.class)
	@ParameterizedTest
	void testParseHeaderWithClass(ParseHeaderClassAliasTest test)
	{
		ContentType contentType = Formats.parseHeader(test._contentType, test._class);
		assertNotNull(contentType);
		assertEquals(test._expectedType, contentType.toString());
	}

	enum ParseHeaderClassAliasTest
	{
		COUNTY_DEFAULT(County.class, Formats.DEFAULT, Formats.JSONV2),
		COUNTY_JSON(County.class, Formats.JSON, Formats.JSONV2),
		COUNTY_JSONV2(County.class, Formats.JSONV2, Formats.JSONV2),
		STATE_DEFAULT(State.class, Formats.DEFAULT, Formats.JSONV2),
		STATE_JSON(State.class, Formats.JSON, Formats.JSONV2),
		STATE_JSONV2(State.class, Formats.JSONV2, Formats.JSONV2),
		OFFICE_DEFAULT(Office.class, Formats.JSONV2, Formats.JSONV2),
		OFFICE_JSON(Office.class, Formats.JSONV2, Formats.JSONV2),
		OFFICE_JSONV2(Office.class, Formats.JSONV2, Formats.JSONV2),
		OFFICE_XML(Office.class, Formats.XML, Formats.XMLV2),
		OFFICE_XMLV2(Office.class, Formats.XMLV2, Formats.XMLV2),
		BLOB_DEFAULT(Blob.class, Formats.DEFAULT, Formats.JSONV2),
		BLOB_JSON(Blob.class, Formats.JSON, Formats.JSONV2),
		BLOB_JSONV2(Blob.class, Formats.JSONV2, Formats.JSONV2),
		BLOBS_DEFAULT(Blobs.class, Formats.DEFAULT, Formats.JSONV2),
		BLOBS_JSON(Blobs.class, Formats.JSON, Formats.JSONV2),
		BLOBS_JSONV2(Blobs.class, Formats.JSONV2, Formats.JSONV2),
		CLOB_DEFAULT(Clob.class, Formats.DEFAULT, Formats.JSONV2),
		CLOB_JSON(Clob.class, Formats.JSON, Formats.JSONV2),
		CLOB_JSONV1(Clob.class, Formats.JSONV1, Formats.JSONV1),
		CLOB_JSONV2(Clob.class, Formats.JSONV2, Formats.JSONV2),
		CLOB_XML(Clob.class, Formats.XML, Formats.XMLV2),
		CLOB_XMLV2(Clob.class, Formats.XMLV2, Formats.XMLV2),
		CLOBS_DEFAULT(Clobs.class, Formats.DEFAULT, Formats.JSONV2),
		CLOBS_JSON(Clobs.class, Formats.JSON, Formats.JSONV2),
		CLOBS_JSONV2(Clobs.class, Formats.JSONV2, Formats.JSONV2),
		BASIN_DEFAULT(Basin.class, Formats.DEFAULT, Formats.NAMED_PGJSON),
		BASIN_PGJSON(Basin.class, Formats.PGJSON, Formats.PGJSON),
		BASIN_NAMED_PGJSON(Basin.class, Formats.NAMED_PGJSON, Formats.NAMED_PGJSON),
		;

		final Class<? extends CwmsDTOBase> _class;
		final String _contentType;
		final String _expectedType;

		ParseHeaderClassAliasTest(Class<? extends CwmsDTOBase> aClass, String contentType, String expectedType)
		{
			_class = aClass;
			_contentType = contentType;
			_expectedType = expectedType;
		}
	}

	enum ParseQueryParamTest
	{
		JSON(null, "json", new ContentType(Formats.JSON)),
		NULL(null, null, null),
		EMPTY(null, "", null),
		OFFICE(Office.class, "json", new ContentType(Formats.JSONV2)),
		;
		final Class<? extends CwmsDTOBase> _class;
		final String _contentType;
		final ContentType _expectedType;

		ParseQueryParamTest(Class<? extends CwmsDTOBase> aClass, String contentType, ContentType expectedType)
		{
			_class = aClass;
			_contentType = contentType;
			_expectedType = expectedType;
		}
	}
}