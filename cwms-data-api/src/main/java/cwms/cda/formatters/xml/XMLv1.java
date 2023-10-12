package cwms.cda.formatters.xml;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import cwms.cda.data.dto.Catalog;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.Office;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.OutputFormatter;
import io.javalin.http.InternalServerErrorResponse;
import service.annotations.FormatService;

@FormatService(contentType = Formats.XML, dataTypes = {Office.class,Catalog.class})
public class XMLv1 implements OutputFormatter {
    private static Logger logger = Logger.getLogger(XMLv1.class.getName());
    private JAXBContext context = null;
    private Marshaller mar = null;

    public XMLv1() throws InternalServerErrorResponse{
        try {
            context = JAXBContext.newInstance(XMLv1Office.class,Catalog.class);
            mar = context.createMarshaller();
            mar.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT,Boolean.TRUE);
        } catch( JAXBException jaxb ){
            logger.log(Level.SEVERE, "Unable to build XML Marshaller", jaxb);
            throw new InternalServerErrorResponse("Internal error");
        }

    }

    @Override
    public String getContentType() {
        return Formats.XML;
    }

    @Override
    public String format(CwmsDTOBase dto) {
        try{
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            if( dto instanceof Office ){
                mar.marshal(new XMLv1Office(Arrays.asList((Office)dto)),pw);
                return sw.toString();
            } else {
                mar.marshal(dto,pw);
                return sw.toString();
            }
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
    @SuppressWarnings("unchecked") // we're ALWAYS checking before conversion in this function
    public String format(List<? extends CwmsDTOBase> dtoList) {
        try{
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);

            if( !dtoList.isEmpty() && dtoList.get(0) instanceof Office ){
                mar.marshal(new XMLv1Office((List<Office>)dtoList), pw);
                return sw.toString();
            }
        } catch( Exception err ){
            logger.log(Level.WARNING, "Error doing XML format of office list", err);
            throw new InternalServerErrorResponse("Invalid Parameters");
        }
        throw new UnsupportedOperationException("Unable to process your request");
    }

}
