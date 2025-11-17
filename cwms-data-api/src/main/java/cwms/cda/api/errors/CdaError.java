package cwms.cda.api.errors;

import io.swagger.v3.oas.annotations.media.Schema;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Class for reporting error to users, primary used for default error handlers for exceptions,
 * however you can initialize and return more detail to the user if it makes sense for the given
 * endpoint.
 */
public final class CdaError {
    private static final String UNKNOWN_SOURCE = "Unknown";
    private final String message;
    @Schema(description = "A randomly generated UUID to help identify your request in the logs "
            + "for analysis..")
    private final String incidentIdentifier;
    private final String source;
    private final Map<String, Serializable> details;

    public String getMessage() {
        return this.message;
    }

    /**
     * randomly generated UUID used for lookups in logs.
     * @return incident identifier
     */
    public String getIncidentIdentifier() {
        return incidentIdentifier;
    }

    /**
     * Key value pairs of additional detail. Such as Object properties that are incorrectly specified.
     * @return Map of details
     */
    public Map<String, Serializable> getDetails() {
        return details;
    }

    public String getSource() {
        return source;
    }

    /**
     * Simple Constructor with just a message.
     * @param message the error message
     */
    public CdaError(String message) {
        this.incidentIdentifier = UUID.randomUUID().toString();
        this.message = message;
        this.details = Collections.unmodifiableMap(new HashMap<>());
        this.source = UNKNOWN_SOURCE;
    }

    /**
     * Constructor with message and detail map.
     * @param message the error message
     * @param map additional details about the error
     */
    public CdaError(String message, Map<String, Serializable> map) {
        Objects.requireNonNull(map);
        this.incidentIdentifier = UUID.randomUUID().toString();
        this.message = message;
        this.details = Collections.unmodifiableMap(map);
        this.source = UNKNOWN_SOURCE;
    }

    /**
     * Constructor with message and detail map, with option to suppress incident ID generation.
     * @param message the error message
     * @param details additional details about the error
     * @param suppressIncidentId if true, suppresses the incident ID generation
     */
    public CdaError(String message, Map<String, Serializable> details,
                      boolean suppressIncidentId) {
        if (suppressIncidentId) {
            this.incidentIdentifier = "user input error";
        } else {
            this.incidentIdentifier = UUID.randomUUID().toString();
        }
        this.message = message;
        this.details = Collections.unmodifiableMap(details);
        this.source = UNKNOWN_SOURCE;
    }

    /**
     * Simple Error that doesn't require an incident ID.
     * @param message the error message
     * @param suppressIncidentId if true, suppresses the incident ID generation
     */
    public CdaError(String message, boolean suppressIncidentId) {
        this(message,new HashMap<>(),suppressIncidentId);
    }

    /**
     * Full constructor.
     * @param message the error message
     * @param source the source of the error
     * @param details additional details about the error
     */
    public CdaError(String message, String source, Map<String, Serializable> details) {
        this.incidentIdentifier = UUID.randomUUID().toString();
        this.message = message;
        this.source = source;
        this.details = Collections.unmodifiableMap(details);
    }

    @Override
    public String toString() {
        String result;
        if (source != null) {
            result = String.format("%s: %s. Originates from %s", incidentIdentifier, message, source);
        } else {
            result = String.format("%s: %s.", incidentIdentifier, message);
        }
        return result;
    }

    public static CdaError notImplemented() {
        return new CdaError("Not Implemented");
    }
}
