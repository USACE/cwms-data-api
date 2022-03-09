package cwms.radar.api.errors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class RequiredFieldException extends RuntimeException{
    private Map<String,ArrayList<String>> details = new HashMap<>();

    private RequiredFieldException(){
        super();
        details.put("missing fields",new ArrayList<String>());
    }

    public RequiredFieldException(String field) {
        this();
        details.get("missing fields").add(field);
    }

    public RequiredFieldException(ArrayList<String> fields){
        this();
        details.get("missing fields").addAll(fields);
    }

    public Map<String,ArrayList<String>> getDetails() {
        return details;
    }
}
