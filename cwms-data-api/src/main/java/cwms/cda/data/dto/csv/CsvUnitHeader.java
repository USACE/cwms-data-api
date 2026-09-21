package cwms.cda.data.dto.csv;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to indicate that a field should have units included in its CSV header.
 * For example, if a field "value" is annotated with @CsvUnitHeader, and the DTO
 * has a "units" field with value "ft", the CSV header for this column will be
 * "value (ft)".
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface CsvUnitHeader {
    /**
     * The name(s) of the field(s) that should have units included in their CSV header.
     * The units will be retrieved from the field/method annotated with this annotation.
     */
    String field();
}
