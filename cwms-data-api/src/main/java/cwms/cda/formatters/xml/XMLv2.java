package cwms.cda.formatters.xml;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import cwms.cda.formatters.OutputFormatter;
import io.javalin.http.InternalServerErrorResponse;
import org.apache.commons.io.input.XmlStreamReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;

public class XMLv2 implements OutputFormatter {
    private static final Logger logger = Logger.getLogger(XMLv2.class.getName());

    public XMLv2() {
    }

    @Override
    public String getContentType() {
        return Formats.XMLV2;
    }

    @Override
    public String format(CwmsDTOBase dto) {
        try {
            final JAXBContext context = JAXBContext.newInstance(dto.getClass());
            final Marshaller mar = context.createMarshaller();
            mar.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            mar.marshal(dto, pw);
            return sw.toString();
        } catch (JAXBException jaxb) {
            String msg = dto != null ?
                    "Error rendering '" + dto + "' to XML"
                    :
                    "Null element passed to formatter";
            logger.log(Level.WARNING, msg, jaxb);
            throw new InternalServerErrorResponse("Invalid Parameters");
        }
    }

    @Override
    public String format(List<? extends CwmsDTOBase> dtoList) {
        throw new UnsupportedOperationException("Unable to process your request");
    }

    @Override
    public <T extends CwmsDTOBase> T parseContent(String content, Class<T> type) {
        try (StringReader stringReader = new StringReader(content)) {
            JAXBContext jaxbContext = JAXBContext.newInstance(type);
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
            StreamSource streamSource = new StreamSource(stringReader);
            return unmarshaller.unmarshal(streamSource, type).getValue();
        } catch (Exception e) {
            throw new FormattingException("Could not deserialize:" + content, e);
        }
    }

    @Override
    public <T extends CwmsDTOBase> T parseContent(InputStream content, Class<T> type) {
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(TimeSeries.class);
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
            return unmarshaller.unmarshal(new StreamSource(content), type).getValue();
        } catch (Exception e) {
            throw new FormattingException("Could not deserialize:" + content, e);
        }
    }
}
