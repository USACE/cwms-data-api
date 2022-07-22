package cwms.radar.data.dto;

import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.json.JsonV1;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.radar.data.dao.TimeSeriesDaoImpl;
import cwms.radar.formatters.json.JsonV2;
import org.junit.jupiter.api.Test;

import hec.data.VerticalDatumException;

import static cwms.radar.data.dao.JsonRatingUtilsTest.readFully;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VerticalDatumInfoTest
{

	@Test
	void xml_deserialize () throws JAXBException
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

		VerticalDatumInfo vdi = parseXml(body);
		assertNotNull(vdi);

		VerticalDatumInfo expected = buildVerticalDatumInfo();
		assertVDIEquals(expected, vdi);

	}

	private VerticalDatumInfo parseXml(String body) throws JAXBException
	{
		JAXBContext jaxbContext = JAXBContext.newInstance(VerticalDatumInfo.class);
		Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
		return (VerticalDatumInfo) unmarshaller.unmarshal(new StringReader(body));
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
	void xml_serialize() throws JAXBException
	{
		VerticalDatumInfo expected = buildVerticalDatumInfo();
		assertNotNull(expected);

		String body = getXml(expected);
		assertNotNull(body);

		VerticalDatumInfo actual = parseXml(body);
		assertVDIEquals(expected, actual);
	}

	private String getXml(VerticalDatumInfo vdi) throws JAXBException
	{
		JAXBContext context = JAXBContext.newInstance(VerticalDatumInfo.class);
		Marshaller marshaller = context.createMarshaller();
		marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT,Boolean.TRUE);
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		marshaller.marshal(vdi,pw);
		return sw.toString();
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
	void testVertDatum1() throws VerticalDatumException, IOException
	{
		InputStream stream = getClass().getClassLoader().getResourceAsStream("cwms/radar/data/dto/vert1.xml");
		assertNotNull(stream);
		String v = readFully(stream);

		VerticalDatumInfo vdi = TimeSeriesDaoImpl.parseVerticalDatumInfo(v);
		assertNotNull(vdi);

		VerticalDatumInfo.Offset[] offsets = vdi.getOffsets();
		assertNotNull(offsets);
		assertEquals(1, offsets.length);

//		VerticalDatumContainer vdc = new VerticalDatumContainer(v);
//		assertNotNull(vdc);

	}

	@Test
	void testVertDatum2() throws VerticalDatumException, IOException
	{
		InputStream stream = getClass().getClassLoader().getResourceAsStream("cwms/radar/data/dto/vert2.xml");
		assertNotNull(stream);
		String v = readFully(stream);

		VerticalDatumInfo vdi = TimeSeriesDaoImpl.parseVerticalDatumInfo(v);
		assertNotNull(vdi);

		VerticalDatumInfo.Offset[] offsets = vdi.getOffsets();
		assertNotNull(offsets);
		assertEquals(2, offsets.length);

//		// Should also be able to build the CWMS class from the same input.
//		VerticalDatumContainer vdc = new VerticalDatumContainer(v);
//		assertNotNull(vdc);
	}

	@Test
	void testVertDatum3() throws VerticalDatumException, IOException, JAXBException
	{
		InputStream stream = getClass().getClassLoader().getResourceAsStream("cwms/radar/data/dto/vert3.xml");
		assertNotNull(stream);
		String v = readFully(stream);

		VerticalDatumInfo vdi = TimeSeriesDaoImpl.parseVerticalDatumInfo(v);
		assertNotNull(vdi);

		VerticalDatumInfo.Offset[] offsets = vdi.getOffsets();
		assertNotNull(offsets);
		assertEquals(3, offsets.length);


		String body = getXml(vdi);
//		assertNotNull(body);
//		assertThat(body, isIdenticalTo(new WhitespaceStrippedSource(Input.from(v).build())));

		VerticalDatumInfo actual = parseXml(body);
		assertVDIEquals(vdi, actual);

	}

	@Test
	void xml_serialize_no_offsets() throws JAXBException
	{

		VerticalDatumInfo.Builder builder = new VerticalDatumInfo.Builder()
				.withOffice("LRL").withUnit("m").withLocation("Buckhorn")
				.withNativeDatum("NGVD-29").withElevation(230.7);

		VerticalDatumInfo vdi = builder.build();

		assertNotNull(vdi);

		String body = getXml(vdi);
		assertNotNull(body);

		VerticalDatumInfo actual = parseXml(body);
		assertVDIEquals(vdi, actual);
	}

	@Test
	void xml_serialize_empty_offsets() throws JAXBException
	{

		VerticalDatumInfo.Builder builder = new VerticalDatumInfo.Builder()
				.withOffice("LRL").withUnit("m").withLocation("Buckhorn")
				.withNativeDatum("NGVD-29").withElevation(230.7)
				.withOffsets(new VerticalDatumInfo.Offset[]{});

		VerticalDatumInfo vdi = builder.build();

		assertNotNull(vdi);

		String body = getXml(vdi);
		assertNotNull(body);

		VerticalDatumInfo actual = parseXml(body);
		assertVDIEquals(vdi, actual);
	}

	@Test
	void json_serialize_empty_offsets() throws JAXBException, JsonProcessingException {

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
	void json_serialize_not_set_offsets_is_empty() throws JAXBException, JsonProcessingException {

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
	void json_serialize_null_means_null() throws JAXBException, JsonProcessingException {

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