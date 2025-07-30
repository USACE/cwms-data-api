package cwms.cda.data.dao.rsql;

import org.jooq.Field;
import org.jooq.Table;

import java.util.Arrays;
import java.util.Locale;

/**
 * Resolves a field name supplied by the client (e.g. "unit_id", "UNIT_ID")
 * to the jOOQ {@link Field} generated for the given {@link Table}.
 * <p>
 * The database returns fully qualified, quoted identifiers such as
 * "CWMS_20"."AV_TSV_DQU"."UNIT_ID"
 * This resolver:
 * • removes quotes,
 * • looks for  "<table name>."  and keeps everything that follows,
 * • compares the result without regard to case.
 */
public class JooqFieldResolver implements FieldResolver {

    private final Table<?> table;
    private final String tableNameCanonical;

    public JooqFieldResolver(Table<?> table) {
        this.table = table;
        // Canonical form (uppercase, no quotes) of the table name once, up front
        this.tableNameCanonical = stripQuotes(table.getName()).toUpperCase(Locale.ROOT);
    }

    @Override
    public Field<Object> resolve(String fieldName) {

        Field<?> field = table.field(fieldName);

        if (field == null) {
            String wanted = canonicalColumn(fieldName);

            field = Arrays.stream(table.fields())
                    .filter(f -> canonicalColumn(f.getName()).equals(wanted))
                    .findFirst()
                    .orElseThrow(() ->
                            new IllegalArgumentException("Unknown field: " + fieldName));
        }

        return (Field<Object>) field;
    }

    /* ----------------------------------------------------- helpers */

    private String canonicalColumn(String rawIdentifier) {
        String noQuotes = stripQuotes(rawIdentifier);
        String upper = noQuotes.toUpperCase(Locale.ROOT);

        // Attempt to locate "<TABLE_NAME>."  and use the remainder
        String needle = tableNameCanonical + ".";
        int idx = upper.lastIndexOf(needle);
        String columnPart;

        if (idx >= 0) {
            columnPart = upper.substring(idx + needle.length());
        } else {
            // Fallback: keep text after the last dot
            int lastDot = upper.lastIndexOf('.');
            columnPart = (lastDot >= 0) ? upper.substring(lastDot + 1) : upper;
        }
        return columnPart;
    }

    private static String stripQuotes(String text) {
        return text.replace("\"", "");
    }
}