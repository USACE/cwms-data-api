package cwms.radar.api.errors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RequiredFieldException extends FieldException{
    private Map<String,List<String>> details = new HashMap<>();

    private RequiredFieldException(){
        super();
        details.put("missing fields",new ArrayList<String>());
    }

    public RequiredFieldException(String field) {
        this();
        details.get("missing fields").add(field);
    }

    public RequiredFieldException(List<String> fields){
        this();
        details.get("missing fields").addAll(fields);
    }

    public Map<String,List<String>> getDetails() {
        return details;
    }
}
