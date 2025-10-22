package cwms.cda.api.errors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExclusiveFieldsException extends FieldException {
    private static final String DETAIL_KEY = "Use only one of";
    private static final String MESSAGE = "Mutually exclusive fields used.";
    private final Map<String, Serializable> details = new HashMap<>();

    private ExclusiveFieldsException() {
        super(MESSAGE);
        details.put(DETAIL_KEY, new ArrayList<>());
    }

    public ExclusiveFieldsException(Set<String> fields) {
        super(MESSAGE, createDetails(fields));
        details.put(DETAIL_KEY, String.join(", ", fields));
    }

    @Override
    public Map<String, Serializable> getDetails() {
        return details;
    }

    private static Map<String, Serializable> createDetails(Set<String> fields) {
        Map<String, Serializable> details = new HashMap<>();
        String fieldString = String.join(", ", fields);
        details.put(DETAIL_KEY, fieldString);
        return details;
    }
}
