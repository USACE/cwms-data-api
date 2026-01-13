package cwms.cda.formatters;

import cwms.cda.api.errors.ApplicationException;
import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Level;
import javax.servlet.http.HttpServletResponse;

public class FormattingException extends ApplicationException {
    private static final Level LOG_LEVEL = Level.SEVERE;

    public FormattingException(String message) {
        super(message, PARSER_SOURCE, "Formatting error:" + message,
            HttpServletResponse.SC_NOT_ACCEPTABLE, LOG_LEVEL, new HashMap<>(), null);
    }

    public FormattingException(String message, Throwable err) {
        super(message, PARSER_SOURCE, "Formatting error:" + message,
            ((err instanceof IOException)
                ? HttpServletResponse.SC_INTERNAL_SERVER_ERROR : HttpServletResponse.SC_NOT_ACCEPTABLE),
            LOG_LEVEL, new HashMap<>(), err);
    }
}
