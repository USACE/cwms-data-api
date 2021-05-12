package cwms.radar.formatters;

import java.util.HashMap;
import java.util.logging.Logger;

import cwms.radar.formatters.csv.CsvV1;
import cwms.radar.formatters.tab.TabV1;
import cwms.radar.formatters.xml.XMLv1;
import io.javalin.http.BadRequestResponse;
import io.javalin.http.InternalServerErrorResponse;

public class FormatFactory {
    private static Logger logger = Logger.getLogger(FormatFactory.class.getName());
    private static HashMap<String,String> type_map =new HashMap<>();
    static{        
        type_map.put("json",Formats.JSON);
        type_map.put("xml",Formats.XML);
        type_map.put("wml2",Formats.WML2);
        type_map.put("tab",Formats.TAB);
        type_map.put("csv",Formats.CSV);
    };    

    public static OutputFormatter formatFor(String contentType) throws InternalServerErrorResponse{
        String formats[] = contentType.split(",");
        for( String format: formats ){
            logger.info("Trying to find formatter for " + format);
            if( format.equalsIgnoreCase(Formats.JSON)){
                return new JsonV1();
            } else if (format.equalsIgnoreCase(Formats.JSONV2)) {
                return new JsonV2();
            } else if (format.equalsIgnoreCase(Formats.TAB)){
                return new TabV1();
            } else if (format.equalsIgnoreCase(Formats.CSV)){
                return new CsvV1();
            } else if (format.equalsIgnoreCase(Formats.XML)){
                return new XMLv1();
            }
        }
        throw new UnsupportedOperationException("Format '" +  contentType + "' is not implemented for this end point");
            
    }

    /**
     * Given the history of RADAR, this function allows the old way to mix with the new way.
     * @param header Accept header value
     * @param queryParam format query parameter value
     * @return an appropriate standard mimetype for lookup
     */
    public static String parseHeaderAndQueryParm(String header, String queryParam){
        String contentType = Formats.JSON;
        if( queryParam != null && !queryParam.isEmpty() ){
            contentType = type_map.get(queryParam);
            if( contentType == null ){                
                throw new UnsupportedOperationException(String.format("Format %s is not implemented for this request",queryParam));
            }
        } else if( header == null ){
            throw new BadRequestResponse("You must set the Accept Header to a valid value");
        } else if( header.equals("*/*") ) {
            contentType = Formats.JSON;
        } else {
            contentType = header;
        }         
        return contentType;
    }
}
