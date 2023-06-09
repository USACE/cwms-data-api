package cwms.cda.formatters.xml;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import cwms.cda.data.dto.Clobs;
import cwms.cda.data.dto.CwmsDTO;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.OutputFormatter;
import io.javalin.http.InternalServerErrorResponse;
import service.annotations.FormatService;

@FormatService(contentType = Formats.XMLV2, dataTypes = {TimeSeries.class, Clobs.class})
public class XMLv2 implements OutputFormatter {
    private static Logger logger = Logger.getLogger(XMLv2.class.getName());
    private JAXBContext context = null;
    private Marshaller mar = null;

    public XMLv2() throws InternalServerErrorResponse{
        try {
            context = JAXBContext.newInstance(TimeSeries.class,Clobs.class);
            mar = context.createMarshaller();
            mar.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT,Boolean.TRUE);
        } catch( JAXBException jaxb ){
            logger.log(Level.SEVERE, "Unable to build XML Marshaller", jaxb);
            throw new InternalServerErrorResponse("Internal error");
        }

    }

    @Override
    public String getContentType() {
        return Formats.XMLV2;
    }

    @Override
    public String format(CwmsDTOBase dto) {
        try{
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            mar.marshal(dto,pw);
            return sw.toString();
        } catch( JAXBException jaxb ){
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
