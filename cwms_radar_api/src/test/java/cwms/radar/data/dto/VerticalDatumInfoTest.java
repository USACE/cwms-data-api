package cwms.radar.data.dto;

import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.radar.formatters.json.JsonV2;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
		VerticalDatumInfo.Offset offset = vdi.getOffset();
		assertNotNull(offset);
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

		assertOffsetEquals(expected.getOffset(), vdi.getOffset());
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
				.withNativeDatum("NGVD-29").withElevation(230.7).withOffset(
				new VerticalDatumInfo.Offset(true, "NAVD-88", -.1666));
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

}