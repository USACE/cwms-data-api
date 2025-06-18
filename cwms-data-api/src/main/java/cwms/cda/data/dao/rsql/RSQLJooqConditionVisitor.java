package cwms.cda.data.dao.rsql;

import cwms.cda.helpers.DateUtils;
import cz.jirutka.rsql.parser.ast.AndNode;
import cz.jirutka.rsql.parser.ast.ComparisonNode;
import cz.jirutka.rsql.parser.ast.ComparisonOperator;
import cz.jirutka.rsql.parser.ast.OrNode;
import cz.jirutka.rsql.parser.ast.RSQLOperators;
import cz.jirutka.rsql.parser.ast.RSQLVisitor;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.impl.DSL;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Inspired by <a href="https://www.baeldung.com/rest-api-search-language-rsql-fiql">...</a>
 *
 */
public class RSQLJooqConditionVisitor implements RSQLVisitor<Condition, Void> {

    private final FieldResolver fieldResolver;

    public RSQLJooqConditionVisitor(FieldResolver fieldResolver) {
        this.fieldResolver = fieldResolver;
    }

    @Override
    public Condition visit(AndNode node, Void param) {
        return node.getChildren().stream()
                .map(n -> n.accept(this, param))
                .reduce(DSL.noCondition(), Condition::and);
    }

    @Override
    public Condition visit(OrNode node, Void param) {
        return node.getChildren().stream()
                .map(n -> n.accept(this, param))
                .reduce(DSL.noCondition(), Condition::or);
    }

    @Override
    public Condition visit(ComparisonNode node, Void param) {
        String selector = node.getSelector();
        ComparisonOperator op = node.getOperator();
        List<String> arguments = node.getArguments();

        Field<Object> field = fieldResolver.resolve(selector);

        // If not doing type conversion, just do this: Object value = arguments.get(0)
        // else....
        String rawValue = arguments.isEmpty() ? null : arguments.get(0);
        if (isNullLiteral(rawValue)) {
            return buildNullCondition(field, op);
        }

        Object value = convert(field, arguments.get(0));
        List<Object> values = convertList(field, arguments);

        return buildCondition(field, op, value, values);
    }

    public static boolean isNullLiteral(String value) {
        return value == null || "null".equalsIgnoreCase(value.trim());
    }

    public static Condition buildNullCondition(Field<Object> field, ComparisonOperator operator) {
        if (RSQLOperators.EQUAL.equals(operator)) {
            return field.isNull();
        }
        if (RSQLOperators.NOT_EQUAL.equals(operator)) {
            return field.isNotNull();
        }
        throw new IllegalArgumentException(
                "Operator " + operator + " is not valid with NULL literal");
    }

    public static Condition buildCondition(Field<Object> field,
                                     ComparisonOperator operator,
                                     Object value,
                                     List<Object> values) {

        if (RSQLOperators.EQUAL.equals(operator))                  return field.eq(value);
        if (RSQLOperators.NOT_EQUAL.equals(operator))              return field.ne(value);
        if (RSQLOperators.GREATER_THAN.equals(operator))           return field.gt(value);
        if (RSQLOperators.GREATER_THAN_OR_EQUAL.equals(operator))  return field.ge(value);
        if (RSQLOperators.LESS_THAN.equals(operator))              return field.lt(value);
        if (RSQLOperators.LESS_THAN_OR_EQUAL.equals(operator))     return field.le(value);
        if (RSQLOperators.IN.equals(operator))                     return field.in(values);
        if (RSQLOperators.NOT_IN.equals(operator))                 return field.notIn(values);

        throw new IllegalArgumentException("Unknown operator: " + operator);
    }


    public static Object convert(Field<?> field, String value) {
        if (value == null || value.trim().isEmpty() || "null".equalsIgnoreCase(value.trim())) {
            return null;
        }

        Class<?> type = field.getType();

        if (type == String.class) return value;
        if (type == Integer.class || type == int.class) return Integer.valueOf(value);
        if (type == Long.class || type == long.class) return Long.valueOf(value);
        if (type == Boolean.class || type == boolean.class) return Boolean.valueOf(value);
        if (type == Double.class || type == double.class) return Double.valueOf(value);
        if (type == BigDecimal.class) return new BigDecimal(value);
        if (type == ZonedDateTime.class) return DateUtils.parseUserDate(value, "UTC");
        if (type == Timestamp.class) {
            ZonedDateTime zdt = DateUtils.parseUserDate(value, "UTC");
         //   return zdt; // we could just return zdt and let jOOQ do the zdt->ts
            return Timestamp.from(zdt.toInstant());
        }

        if (type == UUID.class) return UUID.fromString(value);

        throw new IllegalArgumentException("Unsupported field type: " + type.getName());
    }

    public static List<Object> convertList(Field<?> field, List<String> values) {
        return values.stream()
                .map(value -> convert(field, value))
                .collect(Collectors.toList());
    }

}
