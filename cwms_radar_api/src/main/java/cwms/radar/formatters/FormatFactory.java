package cwms.radar.formatters;

import java.util.HashMap;

import cwms.radar.formatters.csv.CsvV1;
import cwms.radar.formatters.tab.TabV1;
import io.javalin.http.BadRequestResponse;

public class FormatFactory {

    private static HashMap<String,String> type_map =new HashMap<>();
    static{        
        type_map.put("json",Formats.JSON);
        type_map.put("xml",Formats.XML);
        type_map.put("wml2",Formats.WML2);
        type_map.put("tab",Formats.TAB);
        type_map.put("csv",Formats.CSV);
    };

    public static OutputFormatter formatFor(String contentType) {
        if( contentType.equalsIgnoreCase(Formats.JSON)){
            return new JsonV1();
        } else if (contentType.equalsIgnoreCase(Formats.JSONV2)) {
            return new JsonV2();
        } else if (contentType.equalsIgnoreCase(Formats.TAB)){
            return new TabV1();
        } else if (contentType.equalsIgnoreCase(Formats.CSV)){
            return new CsvV1();
        } else {
            return null;
        }        
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
                throw new BadRequestResponse(String.format("Format {} is not implemented for this request",queryParam));
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
