package cwms.cda.formatters.csv;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a single collection (List, Set, etc.) on a DTO as the source of CSV rows.
 *
 * <p>When present, the CSV formatter will serialize the elements of the annotated
 * collection as the CSV rows instead of serializing the DTO itself. Each element's
 * properties included in the CSV are determined by {@link CsvRow} annotations on
 * the element type (or default Jackson rules if none are present).</p>
 *
 * <p>Only one member per class hierarchy should be annotated with {@code @CsvCollectionRows}.
 * If multiple are discovered on the same class or its superclasses, formatting will fail.</p>
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface CsvCollectionRows {
}
