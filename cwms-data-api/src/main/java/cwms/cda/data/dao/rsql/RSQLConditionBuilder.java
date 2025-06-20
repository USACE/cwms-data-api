package cwms.cda.data.dao.rsql;

import cz.jirutka.rsql.parser.RSQLParser;
import cz.jirutka.rsql.parser.ast.Node;
import org.jooq.Condition;

/**
 * A utility class for converting RSQL query strings into jOOQ Conditions.
 * This class can be used from Dao code to parse RSQL query strings and convert them
 * to jOOQ Conditions that can be used in database queries.
 */
public class RSQLConditionBuilder {

    private final FieldResolver fieldResolver;

    /**
     * Creates a new RSQLConditionBuilder with the given field resolver.
     *
     * @param fieldResolver The resolver to use for converting field names to jOOQ Fields
     */
    public RSQLConditionBuilder(FieldResolver fieldResolver) {
        this.fieldResolver = fieldResolver;
    }

    /**
     * Converts an RSQL query string to a jOOQ Condition.
     *
     * @param rsqlQuery The RSQL query string to convert
     * @return The equivalent jOOQ Condition
     * @throws IllegalArgumentException if the query is invalid or contains unknown fields
     */
    public Condition buildCondition(String rsqlQuery) {
        if (rsqlQuery == null || rsqlQuery.trim().isEmpty()) {
            throw new IllegalArgumentException("RSQL query cannot be null or empty");
        }

        // Parse the RSQL query string into a Node
        Node rootNode = new RSQLParser().parse(rsqlQuery);

        // Use the RSQLJooqConditionVisitor to convert the Node to a Condition
        RSQLJooqConditionVisitor visitor = new RSQLJooqConditionVisitor(fieldResolver);
        return rootNode.accept(visitor);
    }

    /**
     * Static factory method to create a new RSQLConditionBuilder with the given field resolver.
     *
     * @param fieldResolver The resolver to use for converting field names to jOOQ Fields
     * @return A new RSQLConditionBuilder
     */
    public static RSQLConditionBuilder create(FieldResolver fieldResolver) {
        return new RSQLConditionBuilder(fieldResolver);
    }
}