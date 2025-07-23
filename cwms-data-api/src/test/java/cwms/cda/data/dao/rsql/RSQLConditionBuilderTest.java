package cwms.cda.data.dao.rsql;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.jooq.Condition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import usace.cwms.db.jooq.codegen.tables.AV_TSV_DQU;

import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

class RSQLConditionBuilderTest {

    @BeforeAll
    static void forceUtc() {
        // When jOOQ converts the Timestamp to a str it seems to use the system default tz.
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        System.setProperty("user.timezone", "UTC");
    }

    @Test
    void testBuildCondition() {
        // Create a field resolver using the JooqFieldResolver
        JooqFieldResolver resolver = new JooqFieldResolver(AV_TSV_DQU.AV_TSV_DQU);
        RSQLConditionBuilder builder = new RSQLConditionBuilder(resolver);

        Condition condition = builder.buildCondition("unit_id==EN;value>25");

        assertNotNull(condition);
        String conditionString = condition.toString();

        assertTrue(conditionString.contains("\"AV_TSV_DQU\".\"UNIT_ID\" = 'EN'"));
        assertTrue(conditionString.contains("\"AV_TSV_DQU\".\"VALUE\" > 2.5E1"));
    }

    @Test
    void testBuildBadConditions() {
        // Create a field resolver using the JooqFieldResolver
        JooqFieldResolver resolver = new JooqFieldResolver(AV_TSV_DQU.AV_TSV_DQU);
        RSQLConditionBuilder builder = new RSQLConditionBuilder(resolver);
        // What happens if a bad user thinks they can use this end-point to inject SQL?
        // SQL injection is NOT going to work with RSQL, but this test demos how it would be handled.
        assertThrows(IllegalArgumentException.class, () -> {
            builder.buildCondition("Robert'; DROP TABLE Students; --");  // Little Bobby Tables we call him
        });

        // zero length
        assertThrows(IllegalArgumentException.class, () -> builder.buildCondition(""));

        // null
        assertThrows(IllegalArgumentException.class, () -> builder.buildCondition(null));

        // single space
        assertThrows(IllegalArgumentException.class, () -> builder.buildCondition(" "));

        // newline
        assertThrows(IllegalArgumentException.class, () -> builder.buildCondition("\n"));

        // C end of string char
        assertThrows(IllegalArgumentException.class, () -> builder.buildCondition("\0"));

        // partially good query with bogus junk on end.
        assertThrows(IllegalArgumentException.class, () -> builder.buildCondition("unit_id==EN;value>25;fhgbass"));

    }

    @Test
    void testBuildConditionWithMapFieldResolver() {
        // Create a map of field names to fields
        Map<String, org.jooq.Field<?>> fieldMap = new HashMap<>();
        fieldMap.put("unit_id", AV_TSV_DQU.AV_TSV_DQU.UNIT_ID);
        fieldMap.put("value", AV_TSV_DQU.AV_TSV_DQU.VALUE);

        // Create a field resolver using the MapFieldResolver
        MapFieldResolver resolver = new MapFieldResolver(fieldMap);

        // Create the RSQLConditionBuilder
        RSQLConditionBuilder builder = new RSQLConditionBuilder(resolver);

        // Build a condition from an RSQL query
        Condition condition = builder.buildCondition("unit_id==EN;value>25");

        // Verify the condition is not null and contains the expected SQL
        assertNotNull(condition);
        String conditionString = condition.toString();

        assertTrue(conditionString.contains("\"AV_TSV_DQU\".\"UNIT_ID\" = 'EN'"));
        assertTrue(conditionString.contains("\"AV_TSV_DQU\".\"VALUE\" > 2.5E1"));
    }

    @Test
    void testBuildConditionWithFactoryMethod() {
        // Create a field resolver using the JooqFieldResolver
        JooqFieldResolver resolver = new JooqFieldResolver(AV_TSV_DQU.AV_TSV_DQU);

        // Create the RSQLConditionBuilder using the factory method
        RSQLConditionBuilder builder = RSQLConditionBuilder.create(resolver);

        // Build a condition from an RSQL query
        Condition condition = builder.buildCondition("unit_id==EN");

        // Verify the condition is not null and contains the expected SQL
        assertNotNull(condition);
        String conditionString = condition.toString();
        assertTrue(conditionString.contains("\"UNIT_ID\" = 'EN'"));
    }

    @Test
    void testBuildConditionWithEmptyQuery() {
        // Create a field resolver using the JooqFieldResolver
        JooqFieldResolver resolver = new JooqFieldResolver(AV_TSV_DQU.AV_TSV_DQU);

        // Create the RSQLConditionBuilder
        RSQLConditionBuilder builder = new RSQLConditionBuilder(resolver);

        // Verify that an empty query throws an IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> builder.buildCondition(""));
        assertThrows(IllegalArgumentException.class, () -> builder.buildCondition(null));
    }

    @Test
    void testBuildConditionWithInOperator() {
        // Create a field resolver using the JooqFieldResolver
        JooqFieldResolver resolver = new JooqFieldResolver(AV_TSV_DQU.AV_TSV_DQU);

        // Create the RSQLConditionBuilder
        RSQLConditionBuilder builder = new RSQLConditionBuilder(resolver);

        // Build a condition from an RSQL query using IN operator
        Condition condition = builder.buildCondition("unit_id=in=(EN,CFS,FEET)");

        // Verify the condition is not null and contains the expected SQL
        assertNotNull(condition);
        String conditionString = condition.toString();

        assertTrue(conditionString.contains("\"UNIT_ID\" in ("));
        assertTrue(conditionString.contains("'EN'"));
        assertTrue(conditionString.contains("'CFS'"));
        assertTrue(conditionString.contains("'FEET'"));
    }

    @Test
    void testBuildConditionWithNotInOperator() {
        // Create a field resolver using the JooqFieldResolver
        JooqFieldResolver resolver = new JooqFieldResolver(AV_TSV_DQU.AV_TSV_DQU);

        // Create the RSQLConditionBuilder
        RSQLConditionBuilder builder = new RSQLConditionBuilder(resolver);

        // Build a condition from an RSQL query using NOT IN operator
        Condition condition = builder.buildCondition("unit_id=out=(EN,CFS)");

        // Verify the condition is not null and contains the expected SQL
        assertNotNull(condition);
        String conditionString = condition.toString();

        assertTrue(conditionString.contains("\"UNIT_ID\" not in ("));
        assertTrue(conditionString.contains("'EN'"));
        assertTrue(conditionString.contains("'CFS'"));
    }

    @Test
    void testBuildConditionWithVersionDate() {
        // Create a field resolver using the JooqFieldResolver
        JooqFieldResolver resolver = new JooqFieldResolver(AV_TSV_DQU.AV_TSV_DQU);

        // Create the RSQLConditionBuilder
        RSQLConditionBuilder builder = new RSQLConditionBuilder(resolver);

        
        // Build a condition from an RSQL query using VERSION_DATE
        Condition condition = builder.buildCondition("version_date==2021-04-05T00:00:00Z");

        // Verify the condition is not null and contains the expected SQL
        assertNotNull(condition);
        String conditionString = condition.toString();

        assertTrue(conditionString.contains("\"AV_TSV_DQU\".\"VERSION_DATE\" = timestamp '2021-04-05 00:00:00.0'"));
    }

    @Test
    void testBuildConditionWithVersionDateRange() {
        JooqFieldResolver resolver = new JooqFieldResolver(AV_TSV_DQU.AV_TSV_DQU);
        RSQLConditionBuilder builder = new RSQLConditionBuilder(resolver);
        Condition condition = builder.buildCondition("version_date>=2021-01-01T00:00:00Z;version_date<=2021-12-31T23:59:59Z");

        // Verify the condition is not null and contains the expected SQL
        assertNotNull(condition);
        String conditionString = condition.toString();

        assertTrue(conditionString.contains("\"AV_TSV_DQU\".\"VERSION_DATE\" >= timestamp '2021-01-01 00:00:00.0'"));
        assertTrue(conditionString.contains("\"AV_TSV_DQU\".\"VERSION_DATE\" <= timestamp '2021-12-31 23:59:59.0'"));
    }

    @Test
    void testBuildConditionComparingFields() {
        Map<String, org.jooq.Field<?>> fieldMap = new HashMap<>();
        fieldMap.put("version_date", AV_TSV_DQU.AV_TSV_DQU.VERSION_DATE);
        fieldMap.put("data_entry_date", AV_TSV_DQU.AV_TSV_DQU.DATA_ENTRY_DATE);
        fieldMap.put("date_time", AV_TSV_DQU.AV_TSV_DQU.DATE_TIME);

        MapFieldResolver mapResolver = new MapFieldResolver(fieldMap);

        RSQLConditionBuilder builder = new RSQLConditionBuilder(mapResolver);

        Condition condition = builder.buildCondition("version_date==2021-04-05T00:00:00Z;data_entry_date>=2021-04-01T00:00:00Z");

        assertNotNull(condition);
        String conditionString = condition.toString();

        assertTrue(conditionString.contains("\"AV_TSV_DQU\".\"VERSION_DATE\" = timestamp '2021-04-05 00:00:00.0'"));
        assertTrue(conditionString.contains("\"AV_TSV_DQU\".\"DATA_ENTRY_DATE\" >= timestamp '2021-04-01 00:00:00.0'"));
    }
}
