package cwms.radar.formatters;


import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FormatsTest
{

	@Test
	public void testParseHeaderAndQueryParmJSON(){
		ContentType contentType =Formats.parseHeaderAndQueryParm("application/json", null);

		assertNotNull(contentType);
		assertEquals("application/json", contentType.getType());
		Map<String, String> parameters = contentType.getParameters();
		assertTrue(parameters == null || parameters.isEmpty());

	}

	@Test
	public void testParseHeaderAndQueryParmJSONv2()
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
	public void testParseNullNull()
	{
		RuntimeException thrown = Assertions.assertThrows(FormattingException.class, () -> {
			Formats.parseHeaderAndQueryParm(null, null);
		});
	}

	@Test
	public void testParseEmptyHeader(){

		ContentType contentType =Formats.parseHeaderAndQueryParm("", "json");

		assertNotNull(contentType);
		assertEquals("application/json", contentType.getType());
		Map<String, String> parameters = contentType.getParameters();
		assertTrue(parameters == null || parameters.isEmpty());
	}

	@Test
	public void testParseNullHeader(){

		ContentType contentType =Formats.parseHeaderAndQueryParm(null, "json");

		assertNotNull(contentType);
		assertEquals("application/json", contentType.getType());
		Map<String, String> parameters = contentType.getParameters();
		assertTrue(parameters == null || parameters.isEmpty());



	}


	@Test
	public void testParseHeaderAndQueryParmXML(){
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
	public void testParseBoth(){
		RuntimeException thrown = Assertions.assertThrows(FormattingException.class, () -> {
			Formats.parseHeaderAndQueryParm("application/json", "json");
		});


	}

	@Test
	public void testParseBothv2(){
		RuntimeException thrown = Assertions.assertThrows(FormattingException.class, () -> {
			Formats.parseHeaderAndQueryParm("application/json;version=2", "json");
		});



	}

	@Test
	public void testParseHeader(){
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
	public void testParseQueryParam(){
		ContentType contentType;
		contentType	= Formats.parseQueryParam("json");
		assertNotNull(contentType);
		assertEquals("application/json", contentType.getType());

		contentType	= Formats.parseQueryParam("");
		assertNull(contentType);

		contentType	= Formats.parseQueryParam(null);
		assertNull(contentType);

	}




}