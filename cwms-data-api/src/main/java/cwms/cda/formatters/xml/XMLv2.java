package cwms.cda.formatters.xml;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.OutputFormatter;
import io.javalin.http.InternalServerErrorResponse;

public class XMLv2 implements OutputFormatter {
    private static Logger logger = Logger.getLogger(XMLv2.class.getName());

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
            mar.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT,Boolean.TRUE);

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            mar.marshal(dto,pw);
            return sw.toString();
        } catch (JAXBException jaxb) {
            String msg = dto != null ?
                    "Error rendering '" + dto.toString() + "' to XML"
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

}
