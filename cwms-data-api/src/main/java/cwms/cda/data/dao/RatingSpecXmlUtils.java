package cwms.cda.data.dao;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;
import cwms.cda.data.dto.rating.IndependentRoundingSpec;
import cwms.cda.data.dto.rating.RatingSpec;
import cwms.cda.formatters.xml.XMLv2;

import java.time.ZonedDateTime;
import java.util.List;


abstract class RatingSpecPlSqlMixin {
    @JacksonXmlProperty(isAttribute = true, localName = "office-id")
    abstract String getOfficeId();

    @JacksonXmlProperty(localName = "rating-spec-id")
    abstract String getRatingId();

    @JacksonXmlElementWrapper(localName = "ind-rounding-specs")
    @JacksonXmlProperty(localName = "ind-rounding-spec")
    abstract IndependentRoundingSpec[] getIndependentRoundingSpecs();

    @JacksonXmlProperty(localName = "dep-rounding-spec")
    abstract String getDependentRoundingSpec();

    @JsonIgnore
    abstract List<ZonedDateTime> getEffectiveDates();
}


class RatingSpecXmlUtils {
    private static final XmlMapper mapper = buildMapper();

    private static XmlMapper buildMapper() {
        XmlMapper m = XMLv2.buildXmlMapper();

        // We don't want to globally mess with how RatingSpec is serialized, just when
        // it comes thru this class.
        m.addMixIn(RatingSpec.class, RatingSpecPlSqlMixin.class);
        m.enable(SerializationFeature.INDENT_OUTPUT);
        m.configure(ToXmlGenerator.Feature.WRITE_XML_DECLARATION, true);
        return m;
    }


    /**
     * The pl/sql create call is expecting a very particular format of xml.
     * CDA publishes a RatingSpec object based on a DTO class.   The CDA RatingSpec class does
     * not quite match what the pl/sql wants.  This method is meant to take a CDA RatingSpec
     * as input and coax it into the format that the pl/sql wants.
     *
     * @param spec The CDA RatingSpec object.
     * @return xml String in the format expected by the pl/sql create call.
     */
    public static String toPlSqlXml(RatingSpec spec) {
        try {
            String xml = mapper.writer()
                    .withRootName("rating-spec")
                    .writeValueAsString(spec);
            
            String namespaces = " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:noNamespaceSchemaLocation=\"http://www.hec.usace.army.mil/xmlSchema/cwms/Ratings.xsd\"";

            // We want to wrap the rating-spec in a ratings element.
            // xml currently starts with <?xml version='1.0' encoding='UTF-8'?>
            // then <rating-spec office-id="...">
            
            int rootStart = xml.indexOf("<rating-spec");
            if (rootStart != -1) {
                String header = xml.substring(0, rootStart);
                String body = xml.substring(rootStart);
                xml = header + "<ratings" + namespaces + ">" + body + "</ratings>";
            }
            
            return xml;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
