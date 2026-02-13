package cwms.cda.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Timestamp;
import java.time.Instant;
import org.jooq.Condition;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

final class AuthorizationFilterHelperTest {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final org.jooq.Field<String> OFFICE_FIELD = DSL.field("OFFICE_ID", String.class);
    private static final org.jooq.Field<Timestamp> TS_FIELD = DSL.field("DATE_TIME", Timestamp.class);
    private static final org.jooq.Field<String> CLASS_FIELD = DSL.field("DATA_CLASSIFICATION", String.class);

    private static boolean originalValue;

    @BeforeAll
    static void enableAccessManagement() throws Exception {
        Field field = AuthorizationContextHelper.class.getDeclaredField("ACCESS_MGMT_ENABLED");
        field.setAccessible(true);

        // Remove the final modifier
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

        originalValue = (boolean) field.get(null);
        field.set(null, true);
    }

    @AfterAll
    static void restoreAccessManagement() throws Exception {
        Field field = AuthorizationContextHelper.class.getDeclaredField("ACCESS_MGMT_ENABLED");
        field.setAccessible(true);
        field.set(null, originalValue);
    }

    private AuthorizationFilterHelper buildHelper(String json) throws Exception {
        JsonNode constraints = json != null ? mapper.readTree(json) : null;
        return new AuthorizationFilterHelper(constraints);
    }

    @Test
    void testNullConstraintsReturnsNoCondition() throws Exception {
        AuthorizationFilterHelper helper = buildHelper(null);
        assertFalse(helper.hasAuthorizationContext());
        Condition condition = helper.getOfficeFilter(OFFICE_FIELD, "SWT");
        assertEquals(DSL.noCondition().toString(), condition.toString());
    }

    @Test
    void testOfficeFilterWildcardAllowsAll() throws Exception {
        AuthorizationFilterHelper helper = buildHelper("{\"allowed_offices\": [\"*\"]}");
        Condition condition = helper.getOfficeFilter(OFFICE_FIELD, "SWT");
        assertEquals(DSL.noCondition().toString(), condition.toString());
    }

    @Test
    void testOfficeFilterRestrictsToAllowed() throws Exception {
        AuthorizationFilterHelper helper = buildHelper("{\"allowed_offices\": [\"SWT\", \"SPK\"]}");
        Condition condition = helper.getOfficeFilter(OFFICE_FIELD, null);
        String sql = condition.toString();
        assertTrue(sql.contains("SWT"), "Should contain allowed office SWT");
        assertTrue(sql.contains("SPK"), "Should contain allowed office SPK");
    }

    @Test
    void testOfficeFilterDeniesUnauthorizedOffice() throws Exception {
        AuthorizationFilterHelper helper = buildHelper("{\"allowed_offices\": [\"SWT\"]}");
        Condition condition = helper.getOfficeFilter(OFFICE_FIELD, "SPK");
        assertEquals(DSL.falseCondition().toString(), condition.toString());
    }

    @Test
    void testOfficeFilterEmptyListDeniesAll() throws Exception {
        AuthorizationFilterHelper helper = buildHelper("{\"allowed_offices\": []}");
        Condition condition = helper.getOfficeFilter(OFFICE_FIELD, null);
        assertEquals(DSL.falseCondition().toString(), condition.toString());
    }

    @Test
    void testEmbargoExemptSkipsFilter() throws Exception {
        AuthorizationFilterHelper helper = buildHelper(
            "{\"embargo_exempt\": true, \"embargo_rules\": {\"SWT\": 72}}");
        Condition condition = helper.getEmbargoFilter(TS_FIELD, OFFICE_FIELD, "SWT");
        assertEquals(DSL.noCondition().toString(), condition.toString());
    }

    @Test
    void testEmbargoFilterAppliesHours() throws Exception {
        AuthorizationFilterHelper helper = buildHelper("{\"embargo_rules\": {\"SWT\": 72}}");
        Condition condition = helper.getEmbargoFilter(TS_FIELD, OFFICE_FIELD, "SWT");
        String sql = condition.toString();
        assertTrue(sql.contains("DATE_TIME"), "Should reference timestamp field");
    }

    @Test
    void testEmbargoFilterNoRulesReturnsNoCondition() throws Exception {
        AuthorizationFilterHelper helper = buildHelper("{}");
        Condition condition = helper.getEmbargoFilter(TS_FIELD, OFFICE_FIELD, "SWT");
        assertEquals(DSL.noCondition().toString(), condition.toString());
    }

    @Test
    void testTsGroupEmbargoZeroHoursNoFilter() throws Exception {
        AuthorizationFilterHelper helper = buildHelper("{\"ts_group_embargo\": {\"public-data\": 0}}");
        Condition condition = helper.getTsGroupEmbargoFilter(TS_FIELD, "public-data");
        assertEquals(DSL.noCondition().toString(), condition.toString());
    }

    @Test
    void testTsGroupEmbargoAppliesHours() throws Exception {
        AuthorizationFilterHelper helper = buildHelper("{\"ts_group_embargo\": {\"restricted-72h\": 72}}");
        Condition condition = helper.getTsGroupEmbargoFilter(TS_FIELD, "restricted-72h");
        String sql = condition.toString();
        assertTrue(sql.contains("DATE_TIME"), "Should reference timestamp field");
    }

    @Test
    void testTsGroupEmbargoUnknownGroupNoFilter() throws Exception {
        AuthorizationFilterHelper helper = buildHelper("{\"ts_group_embargo\": {\"known-group\": 72}}");
        Condition condition = helper.getTsGroupEmbargoFilter(TS_FIELD, "unknown-group");
        assertEquals(DSL.noCondition().toString(), condition.toString());
    }

    @Test
    void testTsGroupEmbargoHoursReturnsValue() throws Exception {
        AuthorizationFilterHelper helper = buildHelper("{\"ts_group_embargo\": {\"restricted-72h\": 72}}");
        int hours = helper.getTsGroupEmbargoHours("restricted-72h");
        assertEquals(72, hours);
    }

    @Test
    void testTsGroupEmbargoHoursUnknownGroupReturnsZero() throws Exception {
        AuthorizationFilterHelper helper = buildHelper("{\"ts_group_embargo\": {\"known-group\": 72}}");
        int hours = helper.getTsGroupEmbargoHours("unknown-group");
        assertEquals(0, hours);
    }

    @Test
    void testClassificationFilterAllowsSpecified() throws Exception {
        AuthorizationFilterHelper helper = buildHelper(
            "{\"data_classification\": [\"public\", \"internal\"]}");
        Condition condition = helper.getClassificationFilter(CLASS_FIELD);
        String sql = condition.toString();
        assertTrue(sql.contains("public"), "Should contain public classification");
        assertTrue(sql.contains("internal"), "Should contain internal classification");
    }

    @Test
    void testClassificationFilterEmptyDeniesAll() throws Exception {
        AuthorizationFilterHelper helper = buildHelper("{\"data_classification\": []}");
        Condition condition = helper.getClassificationFilter(CLASS_FIELD);
        assertEquals(DSL.falseCondition().toString(), condition.toString());
    }

    @Test
    void testTimeWindowAppliesRestriction() throws Exception {
        AuthorizationFilterHelper helper = buildHelper("{\"time_window\": {\"restrict_hours\": 8}}");
        Condition condition = helper.getTimeWindowFilter(TS_FIELD, null);
        String sql = condition.toString();
        assertTrue(sql.contains("DATE_TIME"), "Should reference timestamp field");
    }

    @Test
    void testTimeWindowNoRestrictionReturnsNoCondition() throws Exception {
        AuthorizationFilterHelper helper = buildHelper("{}");
        Condition condition = helper.getTimeWindowFilter(TS_FIELD, null);
        assertEquals(DSL.noCondition().toString(), condition.toString());
    }

    @Test
    void testTimeWindowUserRequestWithinWindow() throws Exception {
        AuthorizationFilterHelper helper = buildHelper("{\"time_window\": {\"restrict_hours\": 8}}");
        Timestamp recentTime = Timestamp.from(Instant.now().minusSeconds(3600));
        Condition condition = helper.getTimeWindowFilter(TS_FIELD, recentTime);
        String sql = condition.toString();
        assertTrue(sql.contains("DATE_TIME"), "Should reference timestamp field");
    }
}
