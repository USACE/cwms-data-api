package cwms.radar.api.formats;

import java.util.List;

import cwms.radar.data.dao.Office;
import io.javalin.plugin.json.JavalinJson;


public class FormatFactory {

    public static FormatResult format(String contentType, List<Office> offices) {
        if( contentType.equalsIgnoreCase("application/json")){
            OfficeFormatV1 ofv1 = new OfficeFormatV1(offices);
            return new FormatResult( JavalinJson.toJson(ofv1),contentType);
        } else if (contentType.equalsIgnoreCase("application/json;version=2")) {
            return new FormatResult( JavalinJson.toJson(offices), contentType);
        } else {
            return null;
        }        
    }
}
