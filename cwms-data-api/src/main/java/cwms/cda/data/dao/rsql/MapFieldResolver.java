package cwms.cda.data.dao.rsql;

import org.jooq.Field;
import java.util.Map;

/**
 * Resolves field names using a pre-built map of field names to jOOQ {@link Field}s.
 */
public class MapFieldResolver implements FieldResolver {
    private final Map<String, Field<?>> nameToField;

    /**
     * Creates a new MapFieldResolver with the given map of field names to fields.
     *
     * @param nameToField A map from field names to jOOQ Fields
     */
    public MapFieldResolver(Map<String, Field<?>> nameToField) {
        this.nameToField = nameToField;
    }

    @Override
    public Field<Object> resolve(String name) {
        Field<?> field = nameToField.get(name);
        if (field == null) {
            throw new IllegalArgumentException("Unknown field: " + name);
        }
        @SuppressWarnings("unchecked")
        Field<Object> typedField = (Field<Object>) field;
        return typedField;
    }
}