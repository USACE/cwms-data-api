package cwms.cda.api.errors;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.logging.Level;

public class NoDataRateException extends RateException{
    // 404 is Not Found - this isn't quite right b/c the rate() or reverse-rate() isn't failing b/c
    // the ts isn't found its failing b/c a necessary rating table isn't found.
    // 424 is Failed Dependency - this is closer to what we want, but it's not quite right either.

    // 422 is Unprocessable Entity.  I think this is the closest.

    public static final int HTTP_ERROR_CODE = 422;

    public NoDataRateException(String message, SQLException cause) {
        super(message, DATABASE_SOURCE, "Error performing rate function: " + message,
                HTTP_ERROR_CODE, Level.INFO, new LinkedHashMap<>(), cause);
    }

}
