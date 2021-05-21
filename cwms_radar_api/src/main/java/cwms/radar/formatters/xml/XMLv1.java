package cwms.radar.formatters.xml;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import cwms.radar.data.dao.CwmsDao;
import cwms.radar.data.dao.Office;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.OutputFormatter;
import io.javalin.http.InternalServerErrorResponse;
import service.annotations.FormatService;

@FormatService(contentType="application/xml", dataTypes = {Office.class})
public class XMLv1 implements OutputFormatter {
    private static Logger logger = Logger.getLogger(XMLv1.class.getName());
    private JAXBContext context = null;
    private Marshaller mar = null;

    public XMLv1() throws InternalServerErrorResponse{
        try {
            context = JAXBContext.newInstance(XMLv1Office.class);
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
    public String format(CwmsDao dao) {
        try{              
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            if( dao instanceof Office ){
                mar.marshal(new XMLv1Office(Arrays.asList((Office)dao)),pw);
                return sw.toString();
            } else {
                return null;
            } 
        } catch( JAXBException jaxb ){
            String msg = dao != null ?
                    "Error rendering '" + dao.toString() + "' to XM"
                    :
                    "Null element passed to formatter";
            logger.log(Level.WARNING, msg, jaxb);
            throw new InternalServerErrorResponse("Invalid Parameters");
        } 
    }

    @Override
    @SuppressWarnings("unchecked") // we're ALWAYS checking before conversion in this function
    public String format(List<? extends CwmsDao> daoList) {
        try{              
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);

            if( !daoList.isEmpty() && daoList.get(0) instanceof Office ){
                mar.marshal(new XMLv1Office((List<Office>)daoList), pw);
                return sw.toString();  
            }    
        } catch( Exception err ){
            logger.log(Level.WARNING, "Error doing XML format of office list", err);
            throw new InternalServerErrorResponse("Invalid Parameters");
        }
        throw new UnsupportedOperationException("Unable to process your request");
    }
    
}
