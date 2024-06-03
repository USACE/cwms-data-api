package cwms.cda.formatters.xml;

import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.Office;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import cwms.cda.formatters.OutputFormatter;
import io.javalin.http.InternalServerErrorResponse;

import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;

public class XMLv1 implements OutputFormatter {
    private static final Logger logger = Logger.getLogger(XMLv1.class.getName());

    public XMLv1() {

    }

    @Override
    public String getContentType() {
        return Formats.XML;
    }

    @Override
    public String format(CwmsDTOBase dto) {
        try {
            final JAXBContext context = JAXBContext.newInstance(dto.getClass());
            final Marshaller mar = context.createMarshaller();
            mar.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            if (dto instanceof Office) {
                mar.marshal(new XMLv1Office(Arrays.asList((Office) dto)), pw);
                return sw.toString();
            } else {
                mar.marshal(dto, pw);
                return sw.toString();
            }
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
    @SuppressWarnings("unchecked") // we're ALWAYS checking before conversion in this function
    public String format(List<? extends CwmsDTOBase> dtoList) {
        try {
            final JAXBContext context = JAXBContext.newInstance(dtoList.getClass());
            final Marshaller mar = context.createMarshaller();
            mar.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);

            if (!dtoList.isEmpty() && dtoList.get(0) instanceof Office) {
                mar.marshal(new XMLv1Office((List<Office>) dtoList), pw);
                return sw.toString();
            }
        } catch (Exception err) {
            logger.log(Level.WARNING, "Error doing XML format of office list", err);
            throw new InternalServerErrorResponse("Invalid Parameters");
        }
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
