package cwms.radar.api.errors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FieldException extends RuntimeException {

    public FieldException(String message) {
        super(message);
    }

    public Map<String, ? extends List<String>> getDetails() {
        return new HashMap<>();
    }
}
