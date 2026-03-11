package cwms.cda.formatters.csv;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation for CSV metadata properties. This can be used by CSV tooling
 * to distinguish dataset-level metadata from row values. No behavior is currently
 * bound to this annotation; it serves documentation purposes and for potential
 * future introspection.
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface CsvMetadata {
}
