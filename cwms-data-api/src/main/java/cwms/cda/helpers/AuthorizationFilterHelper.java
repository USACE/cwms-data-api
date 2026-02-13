package cwms.cda.helpers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.flogger.FluentLogger;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.impl.DSL;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * Applies authorization filtering constraints from x-cwms-auth-context header to database queries.
 * Generates JOOQ conditions for office filtering, embargo rules, time windows, and data classification.
 */
public class AuthorizationFilterHelper {

    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    private static final ObjectMapper mapper = new ObjectMapper();

    private final JsonNode constraints;
    private final boolean hasAuthContext;
    private final boolean shouldEnforce;

    public AuthorizationFilterHelper(io.javalin.http.Context ctx) {
        JsonNode constraintsNode = null;
        boolean hasContext = false;

        try {
            String authHeader = ctx.header("x-cwms-auth-context");
            if (authHeader != null && !authHeader.isEmpty()) {
                JsonNode authContext = mapper.readTree(authHeader);
                constraintsNode = authContext.get("constraints");
                hasContext = true;

                logger.atFine().log("Authorization context loaded with constraints: %s",
                    constraintsNode != null ? constraintsNode.toString() : "none");
            }
        } catch (Exception e) {
            logger.atWarning().withCause(e).log("Failed to parse authorization context");
        }

        this.constraints = constraintsNode;
        this.hasAuthContext = hasContext;
        this.shouldEnforce = AuthorizationContextHelper.isEnabled() && hasContext;

        if (!AuthorizationContextHelper.isEnabled()) {
            logger.atFine().log("Access management is disabled - filters will be bypassed");
        }
    }

    public AuthorizationFilterHelper(JsonNode constraints) {
        this.constraints = constraints;
        this.hasAuthContext = constraints != null;
        this.shouldEnforce = AuthorizationContextHelper.isEnabled() && this.hasAuthContext;
    }

    public boolean hasAuthorizationContext() {
        return hasAuthContext;
    }

    public boolean shouldEnforceAuthorization() {
        return shouldEnforce;
    }

    public Condition getOfficeFilter(Field<String> officeField, String requestedOffice) {
        if (!shouldEnforce) {
            return DSL.noCondition();
        }
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
            logger.atFine().log("User has access to all offices");
            return DSL.noCondition();
        }

        if (allowedOffices.isEmpty()) {
            logger.atWarning().log("User has no allowed offices - denying all access");
            return DSL.falseCondition();
        }

        // User requested a specific office
        if (requestedOffice != null && !requestedOffice.isEmpty()) {
            if (!allowedOffices.contains(requestedOffice)) {
                logger.atWarning().log("User not authorized for office: %s", requestedOffice);
                return DSL.falseCondition();
            }
            return officeField.eq(requestedOffice);
        }

        // Filter to user's allowed offices
        logger.atFine().log("Filtering to allowed offices: %s", allowedOffices);
        return officeField.in(allowedOffices);
    }

    public Condition getEmbargoFilter(Field<Timestamp> timestampField, Field<String> officeField, String requestedOffice) {
        if (!shouldEnforce || constraints == null) {
            return DSL.noCondition();
        }

        boolean embargoExempt = constraints.has("embargo_exempt") &&
                                constraints.get("embargo_exempt").asBoolean();

        if (embargoExempt) {
            logger.atFine().log("User is exempt from embargo rules");
            return DSL.noCondition();
        }

        JsonNode embargoRulesNode = constraints.get("embargo_rules");
        if (embargoRulesNode == null || embargoRulesNode.isNull()) {
            logger.atFine().log("No embargo rules present");
            return DSL.noCondition();
        }

        if (requestedOffice != null && embargoRulesNode.has(requestedOffice)) {
            int embargoHours = embargoRulesNode.get(requestedOffice).asInt();
            Timestamp cutoff = Timestamp.from(Instant.now().minus(embargoHours, ChronoUnit.HOURS));
            logger.atFine().log("Applying %s embargo: %d hours (data before %s)",
                requestedOffice, embargoHours, cutoff);
            return timestampField.lessThan(cutoff);
        }

        if (embargoRulesNode.has("default")) {
            int defaultHours = embargoRulesNode.get("default").asInt();
            Timestamp defaultCutoff = Timestamp.from(Instant.now().minus(defaultHours, ChronoUnit.HOURS));
            logger.atFine().log("Applying default embargo: %d hours (data before %s)",
                defaultHours, defaultCutoff);
            return timestampField.lessThan(defaultCutoff);
        }

        return DSL.noCondition();
    }

    public Condition getTsGroupEmbargoFilter(Field<Timestamp> timestampField, String tsGroupId) {
        if (!shouldEnforce || constraints == null) {
            return DSL.noCondition();
        }

        boolean embargoExempt = constraints.has("embargo_exempt") &&
                                constraints.get("embargo_exempt").asBoolean();

        if (embargoExempt) {
            logger.atFine().log("User is exempt from TS group embargo rules");
            return DSL.noCondition();
        }

        JsonNode tsGroupEmbargoNode = constraints.get("ts_group_embargo");
        if (tsGroupEmbargoNode == null || tsGroupEmbargoNode.isNull()) {
            logger.atFine().log("No ts_group embargo rules present");
            return DSL.noCondition();
        }

        if (tsGroupId != null && tsGroupEmbargoNode.has(tsGroupId)) {
            int embargoHours = tsGroupEmbargoNode.get(tsGroupId).asInt();
            if (embargoHours == 0) {
                logger.atFine().log("TS group %s has no embargo", tsGroupId);
                return DSL.noCondition();
            }
            Timestamp cutoff = Timestamp.from(Instant.now().minus(embargoHours, ChronoUnit.HOURS));
            logger.atFine().log("Applying TS group embargo for %s: %d hours (data before %s)",
                tsGroupId, embargoHours, cutoff);
            return timestampField.lessThan(cutoff);
        }

        logger.atFine().log("TS group not found in user privileges - no embargo enforced");
        return DSL.noCondition();
    }

    public int getTsGroupEmbargoHours(String tsGroupId) {
        if (!shouldEnforce) {
            return 0;
        }
        if (constraints == null) {
            return 0;
        }

        boolean embargoExempt = constraints.has("embargo_exempt") &&
                                constraints.get("embargo_exempt").asBoolean();

        if (embargoExempt) {
            return 0;
        }

        JsonNode tsGroupEmbargoNode = constraints.get("ts_group_embargo");
        if (tsGroupEmbargoNode == null || tsGroupEmbargoNode.isNull()) {
            return 0;
        }

        if (tsGroupId != null && tsGroupEmbargoNode.has(tsGroupId)) {
            return tsGroupEmbargoNode.get(tsGroupId).asInt();
        }

        return 0;
    }

    public Condition getTimeWindowFilter(Field<Timestamp> timestampField, Timestamp userRequestedBeginTime) {
        if (!shouldEnforce || constraints == null || !constraints.has("time_window")) {
            return DSL.noCondition();
        }

        JsonNode timeWindowNode = constraints.get("time_window");
        if (timeWindowNode.isNull() || !timeWindowNode.has("restrict_hours")) {
            return DSL.noCondition();
        }

        int restrictHours = timeWindowNode.get("restrict_hours").asInt();
        Timestamp cutoffTime = Timestamp.from(Instant.now().minus(restrictHours, ChronoUnit.HOURS));

        logger.atInfo().log("Applying time window restriction: %d hours (data after %s)",
            restrictHours, cutoffTime);

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
            logger.atWarning().log("No allowed classifications - denying all access");
            return DSL.falseCondition();
        }

        logger.atFine().log("Filtering to allowed classifications: %s", allowedClassifications);
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
