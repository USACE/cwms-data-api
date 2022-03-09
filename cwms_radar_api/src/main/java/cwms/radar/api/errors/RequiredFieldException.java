package cwms.radar.api.errors;

import java.util.HashMap;
import java.util.Map;

public class RequiredFieldException extends RuntimeException{
    private String field;
    private Map<String,String> details = new HashMap<>();

    public RequiredFieldException(String field) {
        super("Missing Required field " + field);
        this.field = field;
        details.put("Field Missing",field);
    }


    public String getField(){
        return field;
    }

    public Map<String,String> getDetails() {
        return details;
    }
}
