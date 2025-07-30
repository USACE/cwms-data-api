package cwms.cda.data.dao.rsql;

import org.jooq.Field;

@FunctionalInterface
public interface FieldResolver {
    Field<Object> resolve(String fieldName);
}
