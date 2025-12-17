package cwms.cda.api.errors;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import javax.servlet.http.HttpServletResponse;

/**
 * Exception indicating that one or more provided query parameters are not supported
 * for the requested operation. Intended for direct user feedback (HTTP 400).
 * Default CDA_MESSAGE specific to Locations catalog.
 */
public final class UnsupportedParametersException extends ApplicationException
{
    private static final Level LOG_LEVEL = Level.INFO;
    public static final String UNSUPPORTED_QUERY_PARAMETERS = "unsupported query parameters";
    public static final String MESSAGE = "unsupported query parameters present";
    public static final String CDA_MESSAGE = "Unsupported parameter(s) for Locations catalog";
    private final Map<String, Serializable> details = new LinkedHashMap<>();

    public UnsupportedParametersException(List<String> params)
    {
        this(MESSAGE, params);
    }

    public UnsupportedParametersException(String message, List<String> params)
    {
        super(message, USER_INPUT_SOURCE, CDA_MESSAGE, HttpServletResponse.SC_BAD_REQUEST,
                LOG_LEVEL, buildDetailsMap(params), null);
        details.put(UNSUPPORTED_QUERY_PARAMETERS, String.join(",", params));
    }

    // option for controller-specific CDA messages
    public UnsupportedParametersException(String message, String cdaMessage, List<String> params)
    {
        super(message, USER_INPUT_SOURCE, cdaMessage, HttpServletResponse.SC_BAD_REQUEST,
                LOG_LEVEL, buildDetailsMap(params), null);
        details.put(UNSUPPORTED_QUERY_PARAMETERS, String.join(",", params));
    }

    public UnsupportedParametersException(String param)
    {
        this(MESSAGE, List.of(param));
    }

    @Override
    public Map<String, Serializable> getDetails()
    {
        return details;
    }

    private static Map<String, Serializable> buildDetailsMap(List<String> fields)
    {
        Map<String, Serializable> details = new LinkedHashMap<>();
        details.put(UNSUPPORTED_QUERY_PARAMETERS, String.join(",", fields));
        return details;
    }
}