package cwms.cda.helpers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.impl.DSL;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Applies authorization filtering constraints from x-cwms-auth-context header to database queries.
 * Generates JOOQ conditions for office filtering, embargo rules, time windows, and data classification.
 */
public class AuthorizationFilterHelper {

    private static final Logger logger = Logger.getLogger(AuthorizationFilterHelper.class.getName());
    private static final ObjectMapper mapper = new ObjectMapper();

    private final JsonNode constraints;
    private final boolean hasAuthContext;

    public AuthorizationFilterHelper(io.javalin.http.Context ctx) {
        JsonNode constraintsNode = null;
        boolean hasContext = false;

        try {
            String authHeader = ctx.header("x-cwms-auth-context");
            if (authHeader != null && !authHeader.isEmpty()) {
                JsonNode authContext = mapper.readTree(authHeader);
                constraintsNode = authContext.get("constraints");
                hasContext = true;

                logger.log(Level.FINE, "Authorization context loaded with constraints: {0}",
                    constraintsNode != null ? constraintsNode.toString() : "none");
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed to parse authorization context", e);
        }

        this.constraints = constraintsNode;
        this.hasAuthContext = hasContext;
    }

    public AuthorizationFilterHelper(JsonNode constraints) {
        this.constraints = constraints;
        this.hasAuthContext = constraints != null;
    }

    public boolean hasAuthorizationContext() {
        return hasAuthContext;
    }

    public Condition getOfficeFilter(Field<String> officeField, String requestedOffice) {
        if (constraints == null || !constraints.has("allowed_offices")) {
            return DSL.noCondition();
        }

        JsonNode allowedOfficesNode = constraints.get("allowed_offices");
        List<String> allowedOffices = new ArrayList<>();

        if (allowedOfficesNode.isArray()) {
            for (JsonNode office : allowedOfficesNode) {
                allowedOffices.add(office.asText());
            }
        }

        if (allowedOffices.contains("*")) {
            logger.log(Level.FINE, "User has access to all offices");
            return DSL.noCondition();
        }

        if (allowedOffices.isEmpty()) {
            logger.log(Level.WARNING, "User has no allowed offices - denying all access");
            return DSL.falseCondition();
        }

        // User requested a specific office
        if (requestedOffice != null && !requestedOffice.isEmpty()) {
            if (!allowedOffices.contains(requestedOffice)) {
                logger.log(Level.WARNING, "User not authorized for office: {0}", requestedOffice);
                return DSL.falseCondition();
            }
            return officeField.eq(requestedOffice);
        }

        // Filter to user's allowed offices
        logger.log(Level.FINE, "Filtering to allowed offices: {0}", allowedOffices);
        return officeField.in(allowedOffices);
    }

    public Condition getEmbargoFilter(Field<Timestamp> timestampField, Field<String> officeField, String requestedOffice) {
        if (constraints == null) {
            return DSL.noCondition();
        }

        boolean embargoExempt = constraints.has("embargo_exempt") &&
                                constraints.get("embargo_exempt").asBoolean();

        if (embargoExempt) {
            logger.log(Level.FINE, "User is exempt from embargo rules");
            return DSL.noCondition();
        }

        JsonNode embargoRulesNode = constraints.get("embargo_rules");
        if (embargoRulesNode == null || embargoRulesNode.isNull()) {
            logger.log(Level.FINE, "No embargo rules present");
            return DSL.noCondition();
        }

        if (requestedOffice != null && embargoRulesNode.has(requestedOffice)) {
            int embargoHours = embargoRulesNode.get(requestedOffice).asInt();
            Timestamp cutoff = Timestamp.from(Instant.now().minus(embargoHours, ChronoUnit.HOURS));
            logger.log(Level.FINE, "Applying {0} embargo: {1} hours (data before {2})",
                new Object[]{requestedOffice, embargoHours, cutoff});
            return timestampField.lessThan(cutoff);
        }

        if (embargoRulesNode.has("default")) {
            int defaultHours = embargoRulesNode.get("default").asInt();
            Timestamp defaultCutoff = Timestamp.from(Instant.now().minus(defaultHours, ChronoUnit.HOURS));
            logger.log(Level.FINE, "Applying default embargo: {0} hours (data before {1})",
                new Object[]{defaultHours, defaultCutoff});
            return timestampField.lessThan(defaultCutoff);
        }

        return DSL.noCondition();
    }

    public Condition getTimeWindowFilter(Field<Timestamp> timestampField, Timestamp userRequestedBeginTime) {
        if (constraints == null || !constraints.has("time_window")) {
            return DSL.noCondition();
        }

        JsonNode timeWindowNode = constraints.get("time_window");
        if (timeWindowNode.isNull() || !timeWindowNode.has("restrict_hours")) {
            return DSL.noCondition();
        }

        int restrictHours = timeWindowNode.get("restrict_hours").asInt();
        Timestamp cutoffTime = Timestamp.from(Instant.now().minus(restrictHours, ChronoUnit.HOURS));

        logger.log(Level.INFO, "Applying time window restriction: {0} hours (data after {1})",
            new Object[]{restrictHours, cutoffTime});

        // Override user's requested time if it's outside the allowed window
        if (userRequestedBeginTime == null || userRequestedBeginTime.before(cutoffTime)) {
            return timestampField.greaterOrEqual(cutoffTime);
        }

        // User's request is within allowed window
        return timestampField.greaterOrEqual(userRequestedBeginTime);
    }

    public Condition getClassificationFilter(Field<String> classificationField) {
        if (constraints == null || !constraints.has("data_classification")) {
            return DSL.noCondition();
        }

        JsonNode classificationNode = constraints.get("data_classification");
        List<String> allowedClassifications = new ArrayList<>();

        if (classificationNode.isArray()) {
            for (JsonNode classification : classificationNode) {
                allowedClassifications.add(classification.asText());
            }
        }

        if (allowedClassifications.isEmpty()) {
            logger.log(Level.WARNING, "No allowed classifications - denying all access");
            return DSL.falseCondition();
        }

        logger.log(Level.FINE, "Filtering to allowed classifications: {0}", allowedClassifications);
        return DSL.or(
            classificationField.in(allowedClassifications),
            classificationField.isNull()  // Allow data with no classification set
        );
    }

    public Condition getAllFilters(
            Field<String> officeField,
            Field<Timestamp> timestampField,
            Field<String> classificationField,
            String requestedOffice,
            Timestamp userRequestedBeginTime) {

        Condition officeFilter = getOfficeFilter(officeField, requestedOffice);
        Condition embargoFilter = getEmbargoFilter(timestampField, officeField, requestedOffice);
        Condition timeWindowFilter = getTimeWindowFilter(timestampField, userRequestedBeginTime);
        Condition classificationFilter = classificationField != null
            ? getClassificationFilter(classificationField)
            : DSL.noCondition();

        return DSL.and(officeFilter, embargoFilter, timeWindowFilter, classificationFilter);
    }
}
