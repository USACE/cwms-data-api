package cwms.cda.formatters;


import java.util.Arrays;
import java.util.Map;

import cwms.cda.data.dto.County;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.annotations.FormattableWith;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
		RuntimeException thrown = Assertions.assertThrows(FormattingException.class, () -> {
			Formats.parseHeaderAndQueryParm(null, null);
		});
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
		RuntimeException thrown = Assertions.assertThrows(FormattingException.class, () -> {
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
		RuntimeException thrown = Assertions.assertThrows(FormattingException.class, () -> {
			Formats.parseHeaderAndQueryParm("application/json", "json");
		});


	}

	@Test
	void testParseBothv2(){
		RuntimeException thrown = Assertions.assertThrows(FormattingException.class, () -> {
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

		contentType = Formats.parseHeader(null);
		assertNull(contentType);

		contentType = Formats.parseHeader("");
		assertNull(contentType);

	}

	@Test
	void testParseQueryParam(){
		ContentType contentType;
		contentType	= Formats.parseQueryParam("json");
		assertNotNull(contentType);
		assertEquals("application/json", contentType.getType());

		contentType	= Formats.parseQueryParam("");
		assertNull(contentType);

		contentType	= Formats.parseQueryParam(null);
		assertNull(contentType);

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
		COUNTY_DEFAULT(County.class, Formats.DEFAULT),
		COUNTY_JSON(County.class, Formats.JSON);

		final Class<? extends CwmsDTOBase> _class;
		final String _contentType;
		final String _expectedType;

		ParseHeaderClassAliasTest(Class<? extends CwmsDTOBase> aClass, String contentType)
		{
			_class = aClass;
			_contentType = contentType;

			String expectedType = _contentType;

			for (FormattableWith formattableWith : _class.getAnnotationsByType(FormattableWith.class))
			{
				if (Arrays.asList(formattableWith.aliases()).contains(_contentType))
				{
					expectedType = formattableWith.contentType();
				}
			}
			_expectedType = expectedType;
		}
	}
}