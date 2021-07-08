package cwms.radar.formatters.xml;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import cwms.radar.data.dto.Clobs;
import cwms.radar.data.dto.CwmsDTO;
import cwms.radar.data.dto.TimeSeries;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.OutputFormatter;
import io.javalin.http.InternalServerErrorResponse;
import service.annotations.FormatService;

@FormatService(contentType = Formats.XMLV2, dataTypes = {TimeSeries.class, Clobs.class})
public class XMLv2 implements OutputFormatter {
    private static Logger logger = Logger.getLogger(XMLv2.class.getName());
    private JAXBContext context = null;
    private Marshaller mar = null;

    public XMLv2() throws InternalServerErrorResponse{
        try {
            context = JAXBContext.newInstance(TimeSeries.class);
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
    public String format(CwmsDTO dto) {
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
    public String format(List<? extends CwmsDTO> dtoList) {
        throw new UnsupportedOperationException("Unable to process your request");
    }

}
