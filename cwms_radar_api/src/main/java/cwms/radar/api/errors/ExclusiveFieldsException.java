package cwms.radar.api.errors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExclusiveFieldsException extends FieldException {
    private static final String DETAIL_KEY = "Use only one of";
    private Map<String, List<String>> details = new HashMap<>();

    private ExclusiveFieldsException() {
        super("Mutually exclusive fields used.");
        details.put(DETAIL_KEY, new ArrayList<>());
    }

    public ExclusiveFieldsException(List<String> fields) {
        this();
        details.get(DETAIL_KEY).addAll(fields);
    }

    @Override
    public Map<String, ? extends List<String>> getDetails() {
        return details;
    }
}
