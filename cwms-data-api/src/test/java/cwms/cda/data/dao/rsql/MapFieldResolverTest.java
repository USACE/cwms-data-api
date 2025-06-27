package cwms.cda.data.dao.rsql;

import cz.jirutka.rsql.parser.RSQLParser;
import cz.jirutka.rsql.parser.ast.Node;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MapFieldResolverTest {

    @Test
    void testResolve() {
        // Create a map of field names to fields
        Map<String, Field<?>> nameToField = new HashMap<>();
        Field<String> unitIdField = DSL.field("UNIT_ID", String.class);
        Field<Integer> valueField = DSL.field("VALUE", Integer.class);

        nameToField.put("unit_id", unitIdField);
        nameToField.put("value", valueField);

        // Create the resolver
        MapFieldResolver resolver = new MapFieldResolver(nameToField);

        // Test resolving existing fields
        Field<Object> resolvedUnitId = resolver.resolve("unit_id");
        Field<Object> resolvedValue = resolver.resolve("value");

        assertNotNull(resolvedUnitId);
        assertNotNull(resolvedValue);
        assertEquals("UNIT_ID", resolvedUnitId.getName());
        assertEquals("VALUE", resolvedValue.getName());

        // Test resolving a non-existent field
        Exception exception = assertThrows(IllegalArgumentException.class, () -> resolver.resolve("non_existent_field"));

        assertTrue(exception.getMessage().contains("Unknown field: non_existent_field"));
    }

    @Test
    void testWithRSQLJooqConditionVisitor() {
        // Create a map of field names to fields
        Map<String, Field<?>> nameToField = new HashMap<>();
        Field<String> unitIdField = DSL.field("UNIT_ID", String.class);
        Field<Integer> valueField = DSL.field("VALUE", Integer.class);

        nameToField.put("unit_id", unitIdField);
        nameToField.put("value", valueField);

        // Create the resolver
        MapFieldResolver resolver = new MapFieldResolver(nameToField);

        // Parse an RSQL expression
        Node root = new RSQLParser().parse("unit_id==EN;value>25");

        // Use the resolver with RSQLJooqConditionVisitor
        Condition testCondition = root.accept(new RSQLJooqConditionVisitor(resolver));

        // Verify the condition
        assertNotNull(testCondition);
        String condStr = testCondition.toString();
        System.out.println("Condition string: " + condStr);

        // The actual format might be different, so we'll check for the field names and values
        // rather than the exact format
        assertTrue(condStr.contains("UNIT_ID") && condStr.contains("EN"));
        assertTrue(condStr.contains("VALUE") && condStr.contains("25"));
    }
}
