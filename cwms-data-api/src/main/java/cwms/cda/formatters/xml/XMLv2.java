package cwms.cda.formatters.xml;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import cwms.cda.formatters.OutputFormatter;
import io.javalin.http.InternalServerErrorResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

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

        try {
            JacksonXmlModule module = new JacksonXmlModule();
            module.setDefaultUseWrapper(false);
            XmlMapper om = new XmlMapper(module);
            return om.readValue(content, type);
        } catch (JsonProcessingException e) {
            throw new FormattingException("Could not deserialize:" + content, e);
        }
    }

}
