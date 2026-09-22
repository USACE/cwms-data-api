package cwms.cda.formatters.xml;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.Office;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.OutputFormatter;
import io.javalin.http.InternalServerErrorResponse;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.List;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import org.jetbrains.annotations.NotNull;

/**
 * An entire day was spent trying to get FasterXML to behave correctly
 * with XML and JSON annotation... one or the other would break so 
 * I created this. Under the hood jackson-databind-xml does the same thing
 * however it can't unwrap a plain list or provide appropriate naming
 * overrides.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class XMLv2Office implements OutputFormatter {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private static final XmlMapper XML_MAPPER = buildXmlMapper();
    private final XmlMapper om;

    public XMLv2Office(XmlMapper om) {
        this.om = om;
    }

    public XMLv2Office() {
        this.om = XML_MAPPER;
    }

    @Override
    public String getContentType() {
        return Formats.XMLV2;
    }

    @Override
    public String format(CwmsDTOBase dto) {
        try {
            return om.writeValueAsString(dto);
        } catch (JsonProcessingException ex) {
            String msg = dto != null
                    ?
                    "Error rendering '" + dto + "' to XML"
                    :
                    "Null element passed to formatter";
            logger.atWarning().withCause(ex).log(msg);
            throw new InternalServerErrorResponse("Invalid Parameters");
        }
    }

    @Override
    public String format(List<? extends CwmsDTOBase> dtoList) {
        try {
            final StringWriter out = new StringWriter();
            final XMLStreamWriter writer = XMLOutputFactory.newFactory().createXMLStreamWriter(out);
            writer.writeStartDocument("UTF-8", "1.1");
            writer.writeStartElement("offices");
            for (CwmsDTOBase dto: dtoList) {
                Office office = (Office)dto;
                writer.writeStartElement("office");
                writer.writeStartElement("name");
                writer.writeCharacters(office.getName());
                writer.writeEndElement();
                writer.writeStartElement("long-name");
                writer.writeCharacters(office.getLongName());
                writer.writeEndElement();
                writer.writeStartElement("type");
                writer.writeCharacters(office.getType());
                writer.writeEndElement();
                writer.writeStartElement("reports-to");
                writer.writeCharacters(office.getReportsTo());
                writer.writeEndElement();
                writer.writeEndElement();
            }
            writer.writeEndElement();
            writer.writeEndDocument();
            return out.toString();

        } catch (XMLStreamException ex) {
            String msg = dtoList != null
                    ? "Error rendering '" + dtoList + "' to XML"
                    : "Null element passed to formatter";
            logger.atWarning().withCause(ex).log(msg);
            throw new InternalServerErrorResponse("Invalid Parameters");
        }
    }

    @Override
    public <T extends CwmsDTOBase> T parseContent(String content, Class<T> type) {
        throw new UnsupportedOperationException("Parsing is not supported for XML office list");
    }

    @Override
    public <T extends CwmsDTOBase> T parseContent(InputStream content, Class<T> type) {
        throw new UnsupportedOperationException("Parsing is not supported for XML office list");
    }

    private static @NotNull XmlMapper buildXmlMapper() {

        XmlMapper retval = XmlMapper.builder().build();
        retval.findAndRegisterModules();
        retval.registerModule(new JacksonXmlModule());
        // Without these two disables an Instant gets written as 3333333.335000000
        retval.disable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS);
        retval.disable(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS);
        retval.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
        retval.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        retval.registerModule(new JavaTimeModule());

        retval.addMixIn(TimeSeries.class, TimeSeriesXmlMixin.class);
        return retval;
    }

}
