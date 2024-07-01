package cwms.cda.data.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import hec.data.RatingException;
import hec.data.cwmsRating.RatingSet;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.stream.Collectors;
import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import mil.army.usace.hec.cwms.rating.io.xml.RatingXmlFactory;

public class JsonRatingUtils {
    private JsonRatingUtils() {
    }

    public static RatingSet fromJson(String json) throws RatingException {
        String xml;
        try {
            xml = jsonToXml(json);
        } catch (IOException | TransformerException e) {
            throw new RatingException(e);
        }

        return RatingXmlFactory.ratingSet(xml);
    }

    public static String toJson(RatingSet ratingSet) throws RatingException {
        String retval = null;
        if (ratingSet != null) {
            try {
                retval = xmlToJson(RatingXmlFactory.toXml(ratingSet, " "));
            } catch (JsonProcessingException e) {
                throw new RatingException(e);
            }
        }
        return retval;
    }

    public static String jsonToXml(String json) throws IOException, TransformerException {
        ObjectMapper om = new ObjectMapper();

        JsonNode jsonNode = om.readTree(json);

        XmlMapper mapper = new XmlMapper();
        ObjectWriter writer = mapper.writer()
                .withRootName("ratings");
        String xml = writer.writeValueAsString(jsonNode);

        return cleanupXml(xml);
    }

    private static String cleanupXml(String xml) throws TransformerException {
        // Doing this in steps b/c I'm not good enough at xslt to make it happen at once.

        // The way we are writing out json, all the xml attributes were turned into
        // child json fields.  We know certain fields (e.g. office-id, position)
        // should be attributes.
        String resourceLocation = "/cwms/cda/data/rating/remove_office.xsl";
        InputStream resourceAsStream = JsonRatingUtils.class.getResourceAsStream(resourceLocation);
        String officeXsl = readStream(resourceAsStream);

        xml = applyTransform(xml, new StreamSource(new StringReader(officeXsl)));


        String[] additionalAttributes = new String[]{"position", "estimate", "unit",};

        for (String attributeName : additionalAttributes) {
            String template = officeXsl.replace("office-id", attributeName);
            xml = applyTransform(xml, new StreamSource(new StringReader(template)));
        }

        // Value should become an attribute except when its inside offset so it needs
        // a special transform.
        xml = applyTransform(xml, buildSourceFromResource("move_value.xsl"));

        // There is also the issue where the value of some elements was being
        // written as an empty child field.  We manually renamed
        // those to element-value in the json transformation.
        // move_element-value will move the value of element-value
        // back into the value of the parent element.
        xml = applyTransform(xml, buildSourceFromResource("move_element-value.xsl"));

        return xml;
    }

    private static Source buildSourceFromResource(String filename) {
        String resourceLocation = "/cwms/cda/data/rating/" + filename;
        InputStream resourceAsStream = JsonRatingUtils.class.getResourceAsStream(resourceLocation);
        if (resourceAsStream == null) {
            throw new IllegalArgumentException("Could not find resource: " + resourceLocation);
        }

        return new StreamSource(resourceAsStream);
    }

    private static String readStream(InputStream inputStream) {
        return new BufferedReader(new InputStreamReader(inputStream))
                .lines().collect(Collectors.joining("\n"));
    }

    private static String applyTransform(String xml, Source xslt) throws TransformerException {
        TransformerFactory factory = TransformerFactory.newInstance();
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

    public static String xmlToJson(String xml) throws JsonProcessingException {
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
        // There is a weird field with an empty name in the json...
        // Lets find those and rename the empty field to something else.

        json = json.replace("\"\":", "\"element-value\":");

        return json;
    }
}
