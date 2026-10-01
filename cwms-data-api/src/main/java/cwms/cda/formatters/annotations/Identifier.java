package cwms.cda.formatters.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a field as an identifier for the object. This is used to determine which fields are used to uniquely identify an object
 * when performing operations such as PATCH MERGE on collections. Fields marked with this annotation will be used to match
 * existing objects in the collection to the incoming data.
 **/
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Identifier {
}
