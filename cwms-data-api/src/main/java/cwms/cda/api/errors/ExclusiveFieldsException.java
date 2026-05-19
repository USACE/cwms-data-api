package cwms.cda.api.errors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class ExclusiveFieldsException extends FieldException {
    private static final String DETAIL_KEY = "Use only one of";
    private static final String MESSAGE = "Mutually exclusive fields were provided in the request.";
    private final Map<String, Serializable> details = new HashMap<>();

    private ExclusiveFieldsException() {
        super(MESSAGE);
        details.put(DETAIL_KEY, new ArrayList<>());
    }

    public ExclusiveFieldsException(Set<String> fields) {
        super(MESSAGE, createDetails(DETAIL_KEY, fields));
        details.put(DETAIL_KEY, String.join(", ", fields));
    }

    @Override
    public Map<String, Serializable> getDetails() {
        return details;
    }
}
