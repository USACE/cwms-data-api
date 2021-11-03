package cwms.radar.data.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import hec.data.RatingException;
import hec.data.cwmsRating.RatingSet;

public class JsonRatingUtils
{
	private JsonRatingUtils()
	{
	}

	public static RatingSet fromJson(String json) throws RatingException
	{
		String xml;
		try
		{
			xml = jsonToXml(json);
		}
		catch(IOException | TransformerException e)
		{
			throw new RatingException(e);
		}

		return RatingSet.fromXml(xml);
	}

	public static String toJson(RatingSet ratingSet) throws RatingException
	{
		String retval = null;
		if(ratingSet != null)
		{
			try
			{
				retval = xmlToJson(ratingSet.toXmlString(" "));
			}
			catch(JsonProcessingException e)
			{
				throw new RatingException(e);
			}
		}
		return retval;
	}

	public static String jsonToXml(String json) throws IOException, TransformerException
	{
		ObjectMapper om = new ObjectMapper();

		JsonNode jsonNode = om.readTree(json);

		XmlMapper mapper = new XmlMapper();
		ObjectWriter writer = mapper.writer()
				.withRootName("ratings")
				;
		String xml = writer.writeValueAsString(jsonNode);

		return cleanupXml(xml);
	}

	private static String cleanupXml(String xml) throws TransformerException
	{
		// Doing this in steps b/c I'm not good enough at xslt to make it happen at once.

		// The way we are writing out json, all the xml attributes were turned into
		// child json fields.  We know certain fields (e.g. office-id, position)
		// should be attributes.
		// There is also the issue where the value of some elements was being
		// written as an empty child field.  We manually renamed
		// those to element-value move_value will move the value of element-value
		// back into the value of the parent element.
		String[] steps = new String[]{"remove_office.xsl","remove_position.xsl", "move_value.xsl" };

		for(String step : steps)
		{
			xml = applyTransform(xml, buildSourceFromResource(step));
		}

		return xml;
	}

	private static Source buildSourceFromResource(String filename)
	{
		String resourceLocation = "/cwms/radar/data/rating/" + filename;
		InputStream resourceAsStream = JsonRatingUtils.class.getResourceAsStream(resourceLocation);
		return new StreamSource(resourceAsStream);
	}

	private static String applyTransform(String xml, Source xslt) throws TransformerException
	{
		TransformerFactory factory =TransformerFactory.newInstance();
		factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
		factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");

		Transformer transformer = factory.newTransformer(xslt);

		StringReader input = new StringReader(xml);
		StreamSource source = new StreamSource(input);


		StringWriter sw = new StringWriter();
		StreamResult outputResult = new StreamResult(sw);
		transformer.transform(source, outputResult);

		return sw.toString();
	}

	public static String xmlToJson(String xml) throws JsonProcessingException
	{
		XmlMapper mapper = new XmlMapper();
		JsonNode jsonNode = mapper.readTree(xml);

		ObjectMapper om = new ObjectMapper();
		ObjectWriter writer = om.writer();

		String json = writer.writeValueAsString(jsonNode);

		// When converted to json by parsing with XmlMapper
		// and passing the result to ObjectMapper
		// the xml like:
		// 		<ind-rounding-specs>
		//			<ind-rounding-spec position="1">2223456782</ind-rounding-spec>
		//		</ind-rounding-specs>
		// becomes:
		// 		"ind-rounding-specs": {
		// 			"ind-rounding-spec": {
		//				"position": "1",
		//				"": "2223456782"
		//			}
		//		}
		//
		// There is a weird field with an empty name in the json.
		// Lets find those and rename the empty field to something else.

		json = json.replace("\"\":", "\"element-value\":");

		return json;
	}
}
