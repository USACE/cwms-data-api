package cwms.radar.api.errors;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import kotlin.random.Random;

/**
 * Class for reporting error to users, primary used for default error handlers for exceptions,
 * however you can initialize and return more detail to the user if it makes sense for the given
 * endpoint.
 */
public class RadarError {
    private String message;
    @Schema(description = "A randomly generated number to help identify your request in the logs "
            + "for analysis..")
    private String incidentIdentifier;
    private Map<String, ? extends Object> details;

    public String getMessage() {
        return this.message;
    }

    public String getIncidentIdentifier() {
        return incidentIdentifier;
    }

    public Map<String, ? extends Object> getDetails() {
        return Collections.unmodifiableMap(details);
    }


    public RadarError(String message) {
        this.incidentIdentifier = Long.toString(Random.Default.nextLong());
        this.message = message;
        this.details = new HashMap<>();
    }

    public RadarError(String message, Map<String, ? extends Object> map) {
        Objects.requireNonNull(map);
        this.incidentIdentifier = Long.toString(Random.Default.nextLong());
        this.message = message;
        this.details = map;
    }

    public RadarError(String message, Map<String, ? extends Object> details,
                      boolean suppressIncidentId) {
        this.incidentIdentifier = "user input error";
        this.message = message;
        this.details = details;
    }

    @Override
    public String toString() {
        return String.format("%s: %s", incidentIdentifier, message);
    }

    public static RadarError notImplemented() {
        return new RadarError("Not Implemented");
    }

    public static RadarError notAuthorized() {
        return new RadarError("Not Authorized");
    }

}
