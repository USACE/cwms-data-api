package cwms.cda.formatters;

import cwms.cda.api.errors.ApplicationException;
import io.jsonwebtoken.io.IOException;
import java.util.HashMap;
import javax.servlet.http.HttpServletResponse;

public class FormattingException extends ApplicationException {

    public FormattingException(String message) {
        super(message, "Parser", "Formatting error:" + message,
            HttpServletResponse.SC_NOT_ACCEPTABLE, new HashMap<>(), null);
    }

    public FormattingException(String message, Throwable err) {
        super(message, "Parser", "Formatting error:" + message,
            ((err instanceof IOException)
                ? HttpServletResponse.SC_INTERNAL_SERVER_ERROR : HttpServletResponse.SC_NOT_ACCEPTABLE),
            new HashMap<>(), err);
    }
}
