package cwms.cda.formatters.csv;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a DTO property (field or getter) to be included in CSV serialization/deserialization.
 *
 * <p>When at least one property in a class is annotated with {@code @CsvRow}, only the
 * annotated properties are included in CSV processing. If no properties are annotated,
 * the default Jackson property inclusion rules apply.</p>
 *
 * <p>The optional {@code index} attribute can be used to specify the column index to use when
 * generating or consuming CSV without a header row. Index is zero-based. A negative value
 * indicates no explicit index is provided and default ordering applies.</p>
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface CsvRow {
    int index() default -1;
}
