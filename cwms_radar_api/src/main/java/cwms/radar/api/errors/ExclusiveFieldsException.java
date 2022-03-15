package cwms.radar.api.errors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ExclusiveFieldsException extends FieldException {
    private Map<String,ArrayList<String>> details = new HashMap<>();

    private ExclusiveFieldsException(){
        super();
        details.put("use only one of",new ArrayList<String>());
    }

    public ExclusiveFieldsException(ArrayList<String> fields){
        this();
        details.get("missing fields").addAll(fields);
    }

    public Map<String,ArrayList<String>> getDetails() {
        return details;
    }
}
