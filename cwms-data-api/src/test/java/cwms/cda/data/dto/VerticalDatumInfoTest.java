package cwms.cda.data.dto;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.data.dao.TimeSeriesDaoImpl;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.formatters.xml.XMLv1;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

import static cwms.cda.data.dao.JsonRatingUtilsTest.readFully;
import static org.junit.jupiter.api.Assertions.*;

class VerticalDatumInfoTest
{

	@Test
	void xml_deserialize ()
	{
		String body = "<vertical-datum-info office=\"LRL\" unit=\"m\">\n"
				+ "\t  <location>Buckhorn</location>\n"
				+ "\t  <native-datum>NGVD-29</native-datum>\n"
				+ "\t  <elevation>230.7</elevation>\n"
				+ "\t  <offset estimate=\"true\">\n"
				+ "\t    <to-datum>NAVD-88</to-datum>\n"
				+ "\t    <value>-.1666</value>\n"
				+ "\t  </offset>\n"
				+ "\t</vertical-datum-info>";

		VerticalDatumInfo vdi = TimeSeriesDaoImpl.parseVerticalDatumInfo(body);
		assertNotNull(vdi);

		VerticalDatumInfo expected = buildVerticalDatumInfo();
		assertVDIEquals(expected, vdi);

	}

	private void assertMatchesExpected(VerticalDatumInfo vdi)
	{
		assertEquals("LRL", vdi.getOffice());
		assertEquals("m", vdi.getUnit());
		assertEquals("Buckhorn", vdi.getLocation());
		assertEquals("NGVD-29", vdi.getNativeDatum());
		assertEquals(230.7, vdi.getElevation());
		VerticalDatumInfo.Offset[] offsets = vdi.getOffsets();
		assertNotNull(offsets);
		VerticalDatumInfo.Offset offset = offsets[0];
		assertEquals("NAVD-88", offset.getToDatum());
		assertTrue(offset.isEstimate());
		assertEquals(-.1666, offset.getValue());
	}

	private void assertVDIEquals(VerticalDatumInfo expected, VerticalDatumInfo vdi)
	{
		assertEquals(expected.getOffice(), vdi.getOffice());
		assertEquals(expected.getUnit(), vdi.getUnit());
		assertEquals(expected.getLocation(), vdi.getLocation());
		assertEquals(expected.getNativeDatum(), vdi.getNativeDatum());
		assertEquals(expected.getElevation(), vdi.getElevation());

		assertOffsetsEquals(expected.getOffsets(), vdi.getOffsets());
	}

	private void assertOffsetsEquals(VerticalDatumInfo.Offset[] offsets, VerticalDatumInfo.Offset[] offsets1)
	{
		if(offsets== null)
		{
			assertNull(offsets1);
			return;
		}
		assertEquals(offsets.length, offsets1.length);
        for (int i = 0; i < offsets.length; i++)
        {
            assertOffsetEquals(offsets[i], offsets1[i]);
        }
	}

	private void assertOffsetEquals(VerticalDatumInfo.Offset expectedOffset, VerticalDatumInfo.Offset offset)
	{
		assertEquals(expectedOffset.getToDatum(), offset.getToDatum());
		assertEquals(expectedOffset.isEstimate(), offset.isEstimate());
		assertEquals(expectedOffset.getValue(), offset.getValue());
	}

	VerticalDatumInfo buildVerticalDatumInfo()
	{
		VerticalDatumInfo.Builder builder = new VerticalDatumInfo.Builder()
				.withOffice("LRL").withUnit("m").withLocation("Buckhorn")
				.withNativeDatum("NGVD-29").withElevation(230.7).withOffsets(new VerticalDatumInfo.Offset[]{
				new VerticalDatumInfo.Offset(true, "NAVD-88", -.1666)});
		return builder.build();
	}

	@Test
	void xml_serialize()
	{
		VerticalDatumInfo expected = buildVerticalDatumInfo();
		assertNotNull(expected);

		String body = new XMLv1().format(expected);
		assertNotNull(body);

		VerticalDatumInfo actual = TimeSeriesDaoImpl.parseVerticalDatumInfo(body);
		assertVDIEquals(expected, actual);
	}

	private VerticalDatumInfo parseJson(String body) throws JsonProcessingException {
		ObjectMapper objectMapper = JsonV2.buildObjectMapper();

		return objectMapper.readValue(body, VerticalDatumInfo.class);
	}

	private String getJson(VerticalDatumInfo vdi) throws JsonProcessingException {

		ObjectMapper objectMapper = JsonV2.buildObjectMapper();

		return objectMapper.writeValueAsString(vdi);
	}


	@Test
	void test_json_roundtrip() throws JsonProcessingException
	{
		VerticalDatumInfo expected = buildVerticalDatumInfo();
		assertNotNull(expected);

		ObjectMapper objectMapper = JsonV2.buildObjectMapper();
		String body = objectMapper.writeValueAsString(expected);
		assertNotNull(body);

		VerticalDatumInfo actual = objectMapper.readValue(body, VerticalDatumInfo.class);

		assertVDIEquals(expected, actual);
	}

	@Test
	void testVertDatum1() throws IOException
	{
		InputStream stream = getClass().getClassLoader().getResourceAsStream("cwms/cda/data/dto/vert1.xml");
		assertNotNull(stream);
		String v = readFully(stream);

		VerticalDatumInfo vdi = TimeSeriesDaoImpl.parseVerticalDatumInfo(v);
		assertNotNull(vdi);

		VerticalDatumInfo.Offset[] offsets = vdi.getOffsets();
		assertNotNull(offsets);
		assertEquals(1, offsets.length);
	}

	@Test
	void testVertDatum2() throws IOException
	{
		InputStream stream = getClass().getClassLoader().getResourceAsStream("cwms/cda/data/dto/vert2.xml");
		assertNotNull(stream);
		String v = readFully(stream);

		VerticalDatumInfo vdi = TimeSeriesDaoImpl.parseVerticalDatumInfo(v);
		assertNotNull(vdi);

		VerticalDatumInfo.Offset[] offsets = vdi.getOffsets();
		assertNotNull(offsets);
		assertEquals(2, offsets.length);
	}

	@Test
	void testVertDatum3() throws IOException
	{
		InputStream stream = getClass().getClassLoader().getResourceAsStream("cwms/cda/data/dto/vert3.xml");
		assertNotNull(stream);
		String v = readFully(stream);

		VerticalDatumInfo vdi = TimeSeriesDaoImpl.parseVerticalDatumInfo(v);
		assertNotNull(vdi);

		VerticalDatumInfo.Offset[] offsets = vdi.getOffsets();
		assertNotNull(offsets);
		assertEquals(3, offsets.length);


		String body = new XMLv1().format(vdi);
		VerticalDatumInfo actual = TimeSeriesDaoImpl.parseVerticalDatumInfo(body);
		assertVDIEquals(vdi, actual);

	}

	@Test
	void xml_serialize_no_offsets()
	{

		VerticalDatumInfo.Builder builder = new VerticalDatumInfo.Builder()
				.withOffice("LRL").withUnit("m").withLocation("Buckhorn")
				.withNativeDatum("NGVD-29").withElevation(230.7);

		VerticalDatumInfo vdi = builder.build();

		assertNotNull(vdi);

		String body = new XMLv1().format(vdi);
		assertNotNull(body);

		VerticalDatumInfo actual = TimeSeriesDaoImpl.parseVerticalDatumInfo(body);
		assertVDIEquals(vdi, actual);
	}

	@Test
	void xml_serialize_empty_offsets()
	{

		VerticalDatumInfo.Builder builder = new VerticalDatumInfo.Builder()
				.withOffice("LRL").withUnit("m").withLocation("Buckhorn")
				.withNativeDatum("NGVD-29").withElevation(230.7)
				.withOffsets(new VerticalDatumInfo.Offset[]{});

		VerticalDatumInfo vdi = builder.build();

		assertNotNull(vdi);

		String body = new XMLv1().format(vdi);
		assertNotNull(body);

		VerticalDatumInfo actual = TimeSeriesDaoImpl.parseVerticalDatumInfo(body);
		assertVDIEquals(vdi, actual);
	}

	@Test
	void json_serialize_empty_offsets() throws JsonProcessingException {

		VerticalDatumInfo.Builder builder = new VerticalDatumInfo.Builder()
				.withOffice("LRL").withUnit("m").withLocation("Buckhorn")
				.withNativeDatum("NGVD-29").withElevation(230.7)
				.withOffsets(new VerticalDatumInfo.Offset[]{});

		VerticalDatumInfo vdi = builder.build();

		assertNotNull(vdi);

		String body = getJson(vdi);
		assertNotNull(body);
		assertTrue(body.contains("\"offsets\":[]"));

		VerticalDatumInfo actual = parseJson(body);
		assertVDIEquals(vdi, actual);
	}

	@Test
	void json_serialize_not_set_offsets_is_empty() throws JsonProcessingException {

		VerticalDatumInfo.Builder builder = new VerticalDatumInfo.Builder()
				.withOffice("LRL").withUnit("m").withLocation("Buckhorn")
				.withNativeDatum("NGVD-29").withElevation(230.7)
				;

		VerticalDatumInfo vdi = builder.build();

		assertNotNull(vdi);

		String body = getJson(vdi);
		assertNotNull(body);
		assertTrue(body.contains("\"offsets\":[]"));

		VerticalDatumInfo actual = parseJson(body);
		assertVDIEquals(vdi, actual);
	}

	@Test
	void json_serialize_null_means_null() throws JsonProcessingException {

		VerticalDatumInfo.Builder builder = new VerticalDatumInfo.Builder()
				.withOffice("LRL").withUnit("m").withLocation("Buckhorn")
				.withNativeDatum("NGVD-29").withElevation(230.7)
				.withOffsets(null)
				;

		VerticalDatumInfo vdi = builder.build();

		assertNotNull(vdi);

		String body = getJson(vdi);
		assertNotNull(body);
		assertTrue(body.contains("\"offsets\":null"));

		VerticalDatumInfo actual = parseJson(body);
		assertVDIEquals(vdi, actual);
	}



}