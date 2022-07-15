package cwms.radar.api.errors;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is to wrap the jackson errors for our user reporting needs.
 */
public class JsonFieldsException extends FieldException {


    private JsonFieldsException(String message) {
        super(message);
    }

    public JsonFieldsException(JsonProcessingException jsonError) {
        this(jsonError.getOriginalMessage());
    }

    @Override
    public Map<String, ? extends List<String>> getDetails() {
        return new HashMap<>();
    }
}