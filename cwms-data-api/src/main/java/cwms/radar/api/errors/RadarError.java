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

    /**
     * randomly generated number used for lookups in logs.
     * @return
     */
    public String getIncidentIdentifier() {
        return incidentIdentifier;
    }

    /**
     * Key value pairs of additional detail. Such as Object properties that are incorrectly specified.
     * @return
     */
    public Map<String, ? extends Object> getDetails() {
        return Collections.unmodifiableMap(details);
    }


    /**
     * Simple Constructor with just a message.
     * @param message
     */
    public RadarError(String message) {
        this.incidentIdentifier = Long.toString(Random.Default.nextLong());
        this.message = message;
        this.details = new HashMap<>();
    }

    /**
     * Constructor with message and detail map.
     * @param message
     * @param map
     */
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

    /**
     * Simple Error that doesn't require an incident ID.
     * @param message
     * @param suppressIncidentId
     */
    public RadarError(String message, boolean suppressIncidentId) {
        this(message,new HashMap<>(),suppressIncidentId);
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
