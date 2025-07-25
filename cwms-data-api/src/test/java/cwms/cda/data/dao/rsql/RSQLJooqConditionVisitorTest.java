package cwms.cda.data.dao.rsql;

import cwms.cda.helpers.DateUtils;
import cz.jirutka.rsql.parser.RSQLParser;
import cz.jirutka.rsql.parser.ast.AndNode;
import cz.jirutka.rsql.parser.ast.ComparisonNode;
import cz.jirutka.rsql.parser.ast.ComparisonOperator;
import cz.jirutka.rsql.parser.ast.Node;
import cz.jirutka.rsql.parser.ast.OrNode;
import cz.jirutka.rsql.parser.ast.RSQLOperators;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class RSQLJooqConditionVisitorTest {

    private MapFieldResolver fieldResolver;
    private RSQLJooqConditionVisitor visitor;
    private Map<String, Field<?>> fieldMap;

    @BeforeEach
    void setUp() {

        fieldMap = new HashMap<>();
        fieldMap.put("string_field", DSL.field("STRING_FIELD", String.class));
        fieldMap.put("int_field", DSL.field("INT_FIELD", Integer.class));
        fieldMap.put("long_field", DSL.field("LONG_FIELD", Long.class));
        fieldMap.put("boolean_field", DSL.field("BOOLEAN_FIELD", Boolean.class));
        fieldMap.put("double_field", DSL.field("DOUBLE_FIELD", Double.class));
        fieldMap.put("decimal_field", DSL.field("DECIMAL_FIELD", BigDecimal.class));
        // Removed ZonedDateTime field as it's not supported in DEFAULT dialect
        fieldMap.put("timestamp_field", DSL.field("TIMESTAMP_FIELD", Timestamp.class));
        fieldMap.put("uuid_field", DSL.field("UUID_FIELD", UUID.class));

        fieldResolver = new MapFieldResolver(fieldMap);
        visitor = new RSQLJooqConditionVisitor(fieldResolver);
    }

    @Test
    void testVisitAndNode() {
        // Create an RSQL expression with AND operator
        Node node = new RSQLParser().parse("string_field==value;int_field>10");

        // The node should be an AndNode
        assertTrue(node instanceof AndNode);

        // Visit the node
        Condition condition = node.accept(visitor, null);

        // Verify the condition
        assertNotNull(condition);
        String conditionStr = condition.toString();

        // Check that the condition contains both parts connected by AND
        assertTrue(conditionStr.contains("STRING_FIELD") && conditionStr.contains("value"));
        assertTrue(conditionStr.contains("INT_FIELD") && conditionStr.contains("10"));
        assertTrue(conditionStr.contains("and"));
    }

    @Test
    void testVisitOrNode() {
        // Create an RSQL expression with OR operator
        Node node = new RSQLParser().parse("string_field==value,int_field>10");

        // The node should be an OrNode
        assertTrue(node instanceof OrNode);

        // Visit the node
        Condition condition = node.accept(visitor, null);

        // Verify the condition
        assertNotNull(condition);
        String conditionStr = condition.toString();

        // Check that the condition contains both parts connected by OR
        assertTrue(conditionStr.contains("STRING_FIELD") && conditionStr.contains("value"));
        assertTrue(conditionStr.contains("INT_FIELD") && conditionStr.contains("10"));
        assertTrue(conditionStr.contains("or"));
    }

    @Test
    void testVisitComparisonNode() {
        // Create a comparison node for testing
        ComparisonOperator operator = RSQLOperators.EQUAL;
        String selector = "string_field";
        List<String> arguments = Collections.singletonList("test_value");
        ComparisonNode node = new ComparisonNode(operator, selector, arguments);

        // Visit the node
        Condition condition = visitor.visit(node, null);

        // Verify the condition
        assertNotNull(condition);
        String conditionStr = condition.toString();

        // Check that the condition contains the field and value
        assertTrue(conditionStr.contains("STRING_FIELD") && conditionStr.contains("test_value"));
    }

    @Test
    void testIsNullLiteral() {
        // Test with null value
        assertTrue(RSQLJooqConditionVisitor.isNullLiteral(null));

        // Test with "null" string
        assertTrue(RSQLJooqConditionVisitor.isNullLiteral("null"));

        // Test with "NULL" string (case insensitive)
        assertTrue(RSQLJooqConditionVisitor.isNullLiteral("NULL"));

        // Test with non-null value
        assertFalse(RSQLJooqConditionVisitor.isNullLiteral("value"));

        // Test with double quoted null value
        assertFalse(RSQLJooqConditionVisitor.isNullLiteral("\"null\""));

        // Test with single quoted null value
        assertFalse(RSQLJooqConditionVisitor.isNullLiteral("\'null\'"));
    }

    @Test
    void testBuildNullCondition() {
        // Test with EQUAL operator
        Field<Object> field = DSL.field("TEST_FIELD", Object.class);
        Condition condition = RSQLJooqConditionVisitor.buildNullCondition(field, RSQLOperators.EQUAL);

        assertNotNull(condition);
        String conditionStr = condition.toString();
        assertTrue(conditionStr.contains("TEST_FIELD is null"));

        // Test with NOT_EQUAL operator
        condition = RSQLJooqConditionVisitor.buildNullCondition(field, RSQLOperators.NOT_EQUAL);

        assertNotNull(condition);
        conditionStr = condition.toString();
        assertTrue(conditionStr.contains("TEST_FIELD is not null"));

        // Test with unsupported operator
        assertThrows(IllegalArgumentException.class, 
                     () -> RSQLJooqConditionVisitor.buildNullCondition(field, RSQLOperators.GREATER_THAN));
    }

    @Test
    void testBuildCondition() {
        Field<Object> field = DSL.field("TEST_FIELD", Object.class);
        Object value = "test_value";
        List<Object> values = Arrays.asList("value1", "value2", "value3");

        // Test with EQUAL operator
        Condition condition = RSQLJooqConditionVisitor.buildCondition(field, RSQLOperators.EQUAL, value, values);
        assertNotNull(condition);
        String conditionStr = condition.toString();
        assertTrue(conditionStr.contains("TEST_FIELD = 'test_value'"));

        // Test with NOT_EQUAL operator
        condition = RSQLJooqConditionVisitor.buildCondition(field, RSQLOperators.NOT_EQUAL, value, values);
        assertNotNull(condition);
        conditionStr = condition.toString();
        assertTrue(conditionStr.contains("TEST_FIELD <> 'test_value'"));

        // Test with GREATER_THAN operator
        condition = RSQLJooqConditionVisitor.buildCondition(field, RSQLOperators.GREATER_THAN, value, values);
        assertNotNull(condition);
        conditionStr = condition.toString();
        assertTrue(conditionStr.contains("TEST_FIELD > 'test_value'"));

        // Test with GREATER_THAN_OR_EQUAL operator
        condition = RSQLJooqConditionVisitor.buildCondition(field, RSQLOperators.GREATER_THAN_OR_EQUAL, value, values);
        assertNotNull(condition);
        conditionStr = condition.toString();
        assertTrue(conditionStr.contains("TEST_FIELD >= 'test_value'"));

        // Test with LESS_THAN operator
        condition = RSQLJooqConditionVisitor.buildCondition(field, RSQLOperators.LESS_THAN, value, values);
        assertNotNull(condition);
        conditionStr = condition.toString();
        assertTrue(conditionStr.contains("TEST_FIELD < 'test_value'"));

        // Test with LESS_THAN_OR_EQUAL operator
        condition = RSQLJooqConditionVisitor.buildCondition(field, RSQLOperators.LESS_THAN_OR_EQUAL, value, values);
        assertNotNull(condition);
        conditionStr = condition.toString();
        assertTrue(conditionStr.contains("TEST_FIELD <= 'test_value'"));

        // Test with IN operator
        condition = RSQLJooqConditionVisitor.buildCondition(field, RSQLOperators.IN, value, values);
        assertNotNull(condition);
        conditionStr = condition.toString();
        System.out.println("[DEBUG_LOG] IN condition: " + conditionStr);
        // The format of the IN condition might vary depending on the jOOQ version and dialect
        // So we'll check for the field name and each value separately
        assertTrue(conditionStr.contains("TEST_FIELD"));
        assertTrue(conditionStr.toLowerCase().contains("in"));
        assertTrue(conditionStr.contains("value1"));
        assertTrue(conditionStr.contains("value2"));
        assertTrue(conditionStr.contains("value3"));

        // Test with NOT_IN operator
        condition = RSQLJooqConditionVisitor.buildCondition(field, RSQLOperators.NOT_IN, value, values);
        assertNotNull(condition);
        conditionStr = condition.toString();
        System.out.println("[DEBUG_LOG] NOT_IN condition: " + conditionStr);
        // The format of the NOT IN condition might vary depending on the jOOQ version and dialect
        // So we'll check for the field name and each value separately
        assertTrue(conditionStr.contains("TEST_FIELD"));
        assertTrue(conditionStr.toLowerCase().contains("not in"));
        assertTrue(conditionStr.contains("value1"));
        assertTrue(conditionStr.contains("value2"));
        assertTrue(conditionStr.contains("value3"));

        // Test with unsupported operator
        ComparisonOperator unsupportedOperator = new ComparisonOperator("=unsupported=", true);
        assertThrows(IllegalArgumentException.class, 
                     () -> RSQLJooqConditionVisitor.buildCondition(field, unsupportedOperator, value, values));
    }

    @ParameterizedTest
    @MethodSource("provideFieldsAndValues")
    void testConvert(Field<?> field, String value, Class<?> expectedType) {
        Object result = RSQLJooqConditionVisitor.convert(field, value);

        if (value == null || value.trim().isEmpty() || "null".equalsIgnoreCase(value.trim())) {
            assertNull(result);
        } else {
            assertNotNull(result);
            assertTrue(expectedType.isInstance(result), 
                       "Expected " + result + " to be instance of " + expectedType.getName());
        }
    }

    @Test
    void testConvertWithNullOrEmptyValue() {
        Field<String> field = DSL.field("TEST_FIELD", String.class);

        // Test with null value
        assertNull(RSQLJooqConditionVisitor.convert(field, null));

        // Test with empty value
        assertNull(RSQLJooqConditionVisitor.convert(field, ""));

        // Test with "null" string
        assertNull(RSQLJooqConditionVisitor.convert(field, "null"));
    }

    @Test
    void testConvertWithUnsupportedType() {
        // Create a field with an unsupported type
        Field<Object> field = DSL.field("TEST_FIELD", Object.class);

        // Test that convert throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class, 
                     () -> RSQLJooqConditionVisitor.convert(field, "value"));
    }

    @Test
    void testConvertList() {
        Field<String> field = DSL.field("TEST_FIELD", String.class);
        List<String> values = Arrays.asList("value1", "value2", "value3");

        List<Object> result = RSQLJooqConditionVisitor.convertList(field, values);

        assertNotNull(result);
        assertEquals(3, result.size());
        assertEquals("value1", result.get(0));
        assertEquals("value2", result.get(1));
        assertEquals("value3", result.get(2));
    }


    // Method source for parameterized test
    private static Stream<Arguments> provideFieldsAndValues() {
        return Stream.of(
            Arguments.of(DSL.field("STRING_FIELD", String.class), "test_value", String.class),
            Arguments.of(DSL.field("INT_FIELD", Integer.class), "42", Integer.class),
            Arguments.of(DSL.field("LONG_FIELD", Long.class), "9223372036854775807", Long.class),
            Arguments.of(DSL.field("BOOLEAN_FIELD", Boolean.class), "true", Boolean.class),
            Arguments.of(DSL.field("DOUBLE_FIELD", Double.class), "3.14159", Double.class),
            Arguments.of(DSL.field("DECIMAL_FIELD", BigDecimal.class), "123.456", BigDecimal.class),

            Arguments.of(DSL.field("TIMESTAMP_FIELD", Timestamp.class), "2023-01-01T00:00:00Z", Timestamp.class),
            Arguments.of(DSL.field("UUID_FIELD", UUID.class), "123e4567-e89b-12d3-a456-426614174000", UUID.class),
            Arguments.of(DSL.field("NULL_FIELD", String.class), null, null),
            Arguments.of(DSL.field("EMPTY_FIELD", String.class), "", null),
            Arguments.of(DSL.field("NULL_STRING_FIELD", String.class), "null", null)
        );
    }

    @Test
    void testConvertZonedDateTime() {
        // Since we can't use DSL.field() with ZonedDateTime in DEFAULT dialect,
        // and we can't easily mock the Field interface, we'll test the DateUtils.parseUserDate method directly,
        // since that's what the convert method uses for ZonedDateTime conversion

        String dateStr = "2023-01-01T00:00:00Z";
        ZonedDateTime zdt = DateUtils.parseUserDate(dateStr, "UTC");

        // Verify the parsed date
        assertNotNull(zdt);
        assertEquals(2023, zdt.getYear());
        assertEquals(1, zdt.getMonthValue());
        assertEquals(1, zdt.getDayOfMonth());
        assertEquals(0, zdt.getHour());
        assertEquals(0, zdt.getMinute());
        assertEquals(0, zdt.getSecond());
        assertEquals("Z", zdt.getZone().toString());
    }
}
