package cwms.radar.formatters;

import java.util.HashMap;
import java.util.List;

import cwms.radar.api.formats.FormatResult;
import cwms.radar.data.dao.Office;
import io.javalin.plugin.json.JavalinJson;
import io.javalin.http.BadRequestResponse;

public class FormatFactory {

    private static HashMap<String,String> type_map =new HashMap<>();
    static{        
        type_map.put("json","application/json");
        type_map.put("xml","application/xml");
        type_map.put("wml2","applciation/xml+wml2");
        type_map.put("tab","application/tab");
        type_map.put("csv","application/csv");
    };

    public static OutputFormatter formatFor(String contentType) {
        if( contentType.equalsIgnoreCase("application/json")){
            return new JsonV1();
        } else if (contentType.equalsIgnoreCase("application/json;version=2")) {
            return new JsonV2();
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
        String contentType = "application/json";
        if( queryParam != null && !queryParam.isEmpty() ){
            contentType = type_map.get(queryParam);
            if( contentType == null ){                
                throw new BadRequestResponse(String.format("Format {} is not implemented for this request",queryParam));
            }
        } else if( header == null ){
            throw new BadRequestResponse("You must set the Accept Header to a valid value");
        } else if( header.equals("*/*") ) {
            contentType = "application/json";
        } else {
            contentType = header;
        }         
        return contentType;
    }
}
